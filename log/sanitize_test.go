package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

// testUnsafeUserInputStringer is a stand-in for a custom fmt.Stringer
// that exposes attacker-controlled input.
type testUnsafeUserInputStringer struct{ s string }

func (u testUnsafeUserInputStringer) String() string { return u.s }

// testNilSafeError is an error type with a pointer receiver. A typed-nil
// pointer of this type stored in an error interface would panic if
// Error() were called, allowing the typed-nil guard to be tested.
type testNilSafeError struct{ msg string }

func (e *testNilSafeError) Error() string { return e.msg }

// TestDropVsEscapeUnsafeChars compares the output of
// DroppedUnsafeChars and EscapedUnsafeChars side-by-side. For each
// input, the same row shows the result of calling String() on both
// wrappers.
//
// The any-typed input exercises both string and fmt.Stringer code
// paths.
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
		"lone 0x85 byte (invalid UTF-8, would be U+0085 NEL in valid encoding)": {
			// Without the fix, this byte would survive untouched
			// because for/range decodes it as utf8.RuneError which
			// isUnsafe does not flag.
			input:      "abc\x85def",
			wantDrop:   "abcdef",
			wantEscape: "abc\\x85def",
		},
		"lone C1 continuation byte unrelated to NEL (0x82)": {
			input:      "abc\x82def",
			wantDrop:   "abcdef",
			wantEscape: "abc\\x82def",
		},
		"truncated U+2028 prefix (0xe2 0x80 with no continuation byte)": {
			input:      "first\xe2\x80second",
			wantDrop:   "firstsecond",
			wantEscape: "first\\xe2\\x80second",
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
		"typed-nil error renders as \"null\" without panicking": {
			input:      (*testNilSafeError)(nil),
			wantDrop:   "null",
			wantEscape: "null",
		},
		"nil any renders as \"null\" (matches go-kit's encoding of nil)": {
			input:      nil,
			wantDrop:   "null",
			wantEscape: "null",
		},
		"nil error interface renders as \"null\"": {
			// `var err error` becomes a nil interface when boxed into
			// any; the case error: branch of render does not match,
			// so without the isNullValue guard at the top this would
			// fall through to fmt.Sprint(nil) → "<nil>".
			input:      error(nil),
			wantDrop:   "null",
			wantEscape: "null",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.wantDrop, DropUnsafeChars(tc.input).String(), "DroppedUnsafeChars.String()")
			require.Equal(t, tc.wantEscape, EscapeUnsafeChars(tc.input).String(), "EscapedUnsafeChars.String()")
		})
	}
}

// TestDroppedUnsafeChars_MarshalJSON verifies that json.Marshal
// encodes a DroppedUnsafeChars as a JSON string containing the
// sanitized value.
func TestDroppedUnsafeChars_MarshalJSON(t *testing.T) {
	got, err := json.Marshal(DropUnsafeChars("Mozilla\nlevel=info msg=fake\x1b[31m!"))
	require.NoError(t, err)
	require.JSONEq(t, `"Mozillalevel=info msg=fake[31m!"`, string(got))
}

// TestEscapedUnsafeChars_MarshalJSON verifies that json.Marshal
// encodes an EscapedUnsafeChars as a JSON string with unsafe
// characters replaced by printable escape sequences.
//
// JSON escaping is applied on top of the sanitization, so the encoded
// output contains `\\x0a` for a sanitized string containing `\x0a`.
func TestEscapedUnsafeChars_MarshalJSON(t *testing.T) {
	got, err := json.Marshal(EscapeUnsafeChars("Mozilla\nlevel=info msg=fake\x1b[31m!"))
	require.NoError(t, err)
	require.JSONEq(t, `"Mozilla\\x0alevel=info msg=fake\\x1b[31m!"`, string(got))
}

// TestUnsafeChars_NullValue_MarshalJSON verifies that wrapping any
// nil-like value (nil any, nil interface error, typed-nil pointer)
// encodes to the JSON null literal — not the string "null" or "<nil>" —
// matching what go-kit's JSON encoder writes for the same input.
func TestUnsafeChars_NullValue_MarshalJSON(t *testing.T) {
	var typedNil *testNilSafeError
	var nilIface error

	inputs := map[string]any{
		"nil any":             nil,
		"nil error interface": nilIface,
		"typed-nil error ptr": typedNil,
	}

	for inputName, in := range inputs {
		for wrapperName, marshaler := range map[string]json.Marshaler{
			"DroppedUnsafeChars": DropUnsafeChars(in),
			"EscapedUnsafeChars": EscapeUnsafeChars(in),
		} {
			t.Run(inputName+"/"+wrapperName, func(t *testing.T) {
				got, err := json.Marshal(marshaler)
				require.NoError(t, err)
				require.Equal(t, "null", string(got))
			})
		}
	}
}

// benchSink prevents benchmark results from being optimized away by the
// compiler.
var benchSink string

// Sample values shared by the benchmarks and allocation test.
//
// Each value represents a single log field that would be wrapped at a
// logging call site. "clean" is a typical user-agent string, while
// "dirty" includes an ANSI escape sequence and a newline to exercise
// the sanitization path.
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
// represents the baseline cost of logging an unwrapped value; the
// difference from BenchmarkDropUnsafeChars and
// BenchmarkEscapeUnsafeChars approximates the cost of sanitization.
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

// TestZeroAllocsOnCleanStringPath is a regression test that verifies
// String() does not allocate for a clean string input. The wrapper is
// constructed outside the measurement so only String()'s allocation
// behavior is counted.
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

// TestEndToEnd_logfmt verifies that values wrapped with
// DropUnsafeChars or EscapeUnsafeChars are encoded as expected by
// go-kit's logfmt logger.
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
				// Drop yields "MozillaFAKE". Because the result contains no whitespace,
				// logfmt emits it without quotes.
				"drop": "user_agent=MozillaFAKE\n",
				// Escape yields "Mozilla\x0aFAKE" (with a literal backslash). Because
				// the result contains no whitespace, logfmt does not quote it and it
				// reaches the output unchanged.
				"escape": "user_agent=Mozilla\\x0aFAKE\n",
			},
		},
		"custom Stringer rendering user-controlled text is sanitized": {
			value: testUnsafeUserInputStringer{"path=/foo\nFAKE"},
			wantOnWire: map[string]string{
				// Drop yields "path=/fooFAKE". Because the value contains '=',
				// logfmt quotes it in the output.
				"drop": "user_agent=\"path=/fooFAKE\"\n",
				// Escape yields "path=/foo\x0aFAKE". The embedded '=' causes logfmt to
				// quote the value, and the backslash in `\x0a` is escaped within the
				// quoted string.
				"escape": "user_agent=\"path=/foo\\\\x0aFAKE\"\n",
			},
		},
		"typed-nil error renders as null, matching go-kit raw output": {
			value: (*testNilSafeError)(nil),
			wantOnWire: map[string]string{
				"drop":   "user_agent=null\n",
				"escape": "user_agent=null\n",
			},
		},
		"nil any renders as null, matching go-kit raw output": {
			value: nil,
			wantOnWire: map[string]string{
				"drop":   "user_agent=null\n",
				"escape": "user_agent=null\n",
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

// TestEndToEnd_json verifies the final JSON output produced by go-kit
// when logging values wrapped with DropUnsafeChars or
// EscapeUnsafeChars.
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
		"typed-nil error renders as JSON null, matching go-kit raw output": {
			value: (*testNilSafeError)(nil),
			wantOnWire: map[string]string{
				"drop":   `{"user_agent":null}`,
				"escape": `{"user_agent":null}`,
			},
		},
		"nil any renders as JSON null, matching go-kit raw output": {
			value: nil,
			wantOnWire: map[string]string{
				"drop":   `{"user_agent":null}`,
				"escape": `{"user_agent":null}`,
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
