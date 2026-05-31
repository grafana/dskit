package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

// testUnsafeUserInputStringer is a stand-in for a custom type that wraps
// attacker-influenced text behind fmt.Stringer.
type testUnsafeUserInputStringer struct{ s string }

func (u testUnsafeUserInputStringer) String() string { return u.s }

// TestDropVsEscapeUnsafeChars compares the two wrappers side-by-side:
// for every input, the same row shows what DroppedUnsafeChars and
// EscapedUnsafeChars each render via String(). The any-typed input
// exercises both string and Stringer paths through fmt.Sprint.
func TestDropVsEscapeUnsafeChars(t *testing.T) {
	testCases := map[string]struct {
		input      any
		wantDrop   string
		wantEscape string
	}{
		"plain ascii passes through unchanged": {
			input:      "hello world",
			wantDrop:   "hello world",
			wantEscape: "hello world",
		},
		"tab is preserved in both modes": {
			input:      "col1\tcol2",
			wantDrop:   "col1\tcol2",
			wantEscape: "col1\tcol2",
		},
		"log injection via newline": {
			input:      "first line\nlevel=info msg=fake",
			wantDrop:   "first linelevel=info msg=fake",
			wantEscape: "first line\\x0alevel=info msg=fake",
		},
		"ANSI color escape (terminal injection)": {
			input:      "\x1b[31mFAKE ERROR\x1b[0m",
			wantDrop:   "[31mFAKE ERROR[0m",
			wantEscape: "\\x1b[31mFAKE ERROR\\x1b[0m",
		},
		"NUL byte": {
			input:      "abc\x00def",
			wantDrop:   "abcdef",
			wantEscape: "abc\\x00def",
		},
		"DEL (0x7F)": {
			input:      "abc\x7fdef",
			wantDrop:   "abcdef",
			wantEscape: "abc\\x7fdef",
		},
		"C1 control NEL (U+0085)": {
			input:      "abc\u0085def",
			wantDrop:   "abcdef",
			wantEscape: "abc\\x85def",
		},
		"Unicode LINE SEPARATOR (U+2028)": {
			input:      "first\u2028second",
			wantDrop:   "firstsecond",
			wantEscape: "first\\u2028second",
		},
		"bidi override (trojan source attack)": {
			input:      "if (admin)\u202e true \u202d{ deleteAll() }",
			wantDrop:   "if (admin) true { deleteAll() }",
			wantEscape: "if (admin)\\u202e true \\u202d{ deleteAll() }",
		},
		"non-control multi-byte unicode passes through": {
			input:      "héllo 世界",
			wantDrop:   "héllo 世界",
			wantEscape: "héllo 世界",
		},
		"empty string": {
			input:      "",
			wantDrop:   "",
			wantEscape: "",
		},
		"custom Stringer with embedded newline is sanitized": {
			input:      testUnsafeUserInputStringer{"path=/foo\nFAKE"},
			wantDrop:   "path=/fooFAKE",
			wantEscape: "path=/foo\\x0aFAKE",
		},
		"non-Stringer struct: default formatting then sanitize": {
			input:      struct{ X string }{"ab\nc"},
			wantDrop:   "{abc}",
			wantEscape: "{ab\\x0ac}",
		},
		"int passes through (Sprint formats numerically)": {
			input:      42,
			wantDrop:   "42",
			wantEscape: "42",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.wantDrop, DropUnsafeChars(tc.input).String(), "DroppedUnsafeChars.String()")
			require.Equal(t, tc.wantEscape, EscapeUnsafeChars(tc.input).String(), "EscapedUnsafeChars.String()")
		})
	}
}

// TestDroppedUnsafeChars_MarshalJSON verifies that json.Marshal on a
// DroppedUnsafeChars returns a JSON string containing the inner value
// with unsafe characters dropped.
func TestDroppedUnsafeChars_MarshalJSON(t *testing.T) {
	got, err := json.Marshal(DropUnsafeChars("Mozilla\nlevel=info msg=fake\x1b[31m!"))
	require.NoError(t, err)
	require.JSONEq(t, `"Mozillalevel=info msg=fake[31m!"`, string(got))
}

// TestEscapedUnsafeChars_MarshalJSON verifies that json.Marshal on an
// EscapedUnsafeChars returns a JSON string containing the inner value
// with unsafe characters replaced by their printable escape sequences.
// Note that JSON's own backslash escaping doubles ours: the on-wire
// bytes show `\\x0a` for a sanitized string containing `\x0a` (a
// single backslash followed by x0a).
func TestEscapedUnsafeChars_MarshalJSON(t *testing.T) {
	got, err := json.Marshal(EscapeUnsafeChars("Mozilla\nlevel=info msg=fake\x1b[31m!"))
	require.NoError(t, err)
	require.JSONEq(t, `"Mozilla\\x0alevel=info msg=fake\\x1b[31m!"`, string(got))
}

// benchSink prevents the compiler from optimising away benchmark calls.
var benchSink string

// Sample values shared by the benchmarks and the allocation test.
// These represent a single field value a caller would wrap at a log
// call site (a user-agent header is the canonical case): "clean" is a
// well-formed value; "dirty" mixes in an ANSI escape and a newline so
// the strings.Builder path is exercised.
var (
	benchClean = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
	benchDirty = "Mozilla\x1b[31mFAKE\x1b[0m\nlevel=info msg=hacked"
)

func BenchmarkDropUnsafeChars(b *testing.B) {
	b.Run("clean", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = DropUnsafeChars(benchClean).String()
		}
	})
	b.Run("dirty", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = DropUnsafeChars(benchDirty).String()
		}
	})
}

func BenchmarkEscapeUnsafeChars(b *testing.B) {
	b.Run("clean", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = EscapeUnsafeChars(benchClean).String()
		}
	})
	b.Run("dirty", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = EscapeUnsafeChars(benchDirty).String()
		}
	})
}

// BenchmarkRawSprint measures fmt.Sprint(v) on the same inputs. This
// is the work the underlying logger performs per value when no wrapper
// is used; the delta against BenchmarkDropUnsafeChars and
// BenchmarkEscapeUnsafeChars approximates the wrapper's added cost.
func BenchmarkRawSprint(b *testing.B) {
	b.Run("clean", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = fmt.Sprint(benchClean)
		}
	})
	b.Run("dirty", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = fmt.Sprint(benchDirty)
		}
	})
}

// TestZeroAllocsOnCleanStringPath is a regression guard: wrapping a
// clean string and calling String() must not allocate. The wrapper is
// constructed outside the closure so the measurement captures only
// String()'s allocation behaviour, not any string-to-any boxing at the
// call site.
func TestZeroAllocsOnCleanStringPath(t *testing.T) {
	dropped := DropUnsafeChars(benchClean)
	escaped := EscapeUnsafeChars(benchClean)

	require.Zero(t, testing.AllocsPerRun(100, func() {
		benchSink = dropped.String()
	}), "DroppedUnsafeChars.String must not allocate on a clean string input")

	require.Zero(t, testing.AllocsPerRun(100, func() {
		benchSink = escaped.String()
	}), "EscapedUnsafeChars.String must not allocate on a clean string input")
}

// TestEndToEnd_logfmt verifies that values wrapped with DropUnsafeChars
// or EscapeUnsafeChars produce the expected bytes on the wire when
// logged through go-kit's logfmt encoder.
func TestEndToEnd_logfmt(t *testing.T) {
	testCases := map[string]struct {
		value      any
		wantOnWire map[string]string // logger name -> expected bytes
	}{
		"clean string passes through both wrappers unchanged": {
			value: "Mozilla/5.0",
			wantOnWire: map[string]string{
				"drop":   "user_agent=Mozilla/5.0\n",
				"escape": "user_agent=Mozilla/5.0\n",
			},
		},
		"embedded newline is sanitized before reaching the encoder": {
			value: "Mozilla\nFAKE",
			wantOnWire: map[string]string{
				// Drop yields "MozillaFAKE" — no whitespace, so logfmt
				// emits it unquoted.
				"drop": "user_agent=MozillaFAKE\n",
				// Escape yields "Mozilla\x0aFAKE" (literal backslash);
				// logfmt does not quote on backslash alone, so it
				// reaches the wire as-is.
				"escape": "user_agent=Mozilla\\x0aFAKE\n",
			},
		},
		"custom Stringer rendering user-controlled text is sanitized": {
			value: testUnsafeUserInputStringer{"path=/foo\nFAKE"},
			wantOnWire: map[string]string{
				// Drop yields "path=/fooFAKE"; the embedded '=' forces
				// logfmt to quote the whole value.
				"drop": "user_agent=\"path=/fooFAKE\"\n",
				// Escape yields "path=/foo\x0aFAKE"; '=' still forces
				// quoting, and inside the quote logfmt doubles the
				// backslash to keep the output unambiguously parseable.
				"escape": "user_agent=\"path=/foo\\\\x0aFAKE\"\n",
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Run("drop", func(t *testing.T) {
				buf := &bytes.Buffer{}
				logger := log.NewLogfmtLogger(buf)
				require.NoError(t, logger.Log("user_agent", DropUnsafeChars(tc.value)))
				require.Equal(t, tc.wantOnWire["drop"], buf.String())
			})
			t.Run("escape", func(t *testing.T) {
				buf := &bytes.Buffer{}
				logger := log.NewLogfmtLogger(buf)
				require.NoError(t, logger.Log("user_agent", EscapeUnsafeChars(tc.value)))
				require.Equal(t, tc.wantOnWire["escape"], buf.String())
			})
		})
	}
}

// TestEndToEnd_json verifies that values wrapped with DropUnsafeChars
// or EscapeUnsafeChars produce the expected bytes on the wire when
// logged through go-kit's JSON encoder.
func TestEndToEnd_json(t *testing.T) {
	testCases := map[string]struct {
		value      any
		wantOnWire map[string]string
	}{
		"clean string passes through both wrappers unchanged": {
			value: "Mozilla/5.0",
			wantOnWire: map[string]string{
				"drop":   `{"user_agent":"Mozilla/5.0"}`,
				"escape": `{"user_agent":"Mozilla/5.0"}`,
			},
		},
		"embedded newline is sanitized before reaching the encoder": {
			value: "Mozilla\nFAKE",
			wantOnWire: map[string]string{
				"drop":   `{"user_agent":"MozillaFAKE"}`,
				"escape": `{"user_agent":"Mozilla\\x0aFAKE"}`,
			},
		},
		"custom Stringer rendering user-controlled text is sanitized": {
			value: testUnsafeUserInputStringer{"path=/foo\nFAKE"},
			wantOnWire: map[string]string{
				"drop":   `{"user_agent":"path=/fooFAKE"}`,
				"escape": `{"user_agent":"path=/foo\\x0aFAKE"}`,
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Run("drop", func(t *testing.T) {
				buf := &bytes.Buffer{}
				logger := log.NewJSONLogger(buf)
				require.NoError(t, logger.Log("user_agent", DropUnsafeChars(tc.value)))
				require.JSONEq(t, tc.wantOnWire["drop"], buf.String())
			})
			t.Run("escape", func(t *testing.T) {
				buf := &bytes.Buffer{}
				logger := log.NewJSONLogger(buf)
				require.NoError(t, logger.Log("user_agent", EscapeUnsafeChars(tc.value)))
				require.JSONEq(t, tc.wantOnWire["escape"], buf.String())
			})
		})
	}
}
