package log

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"
)

// userInputStringer is a stand-in for a custom type that wraps
// attacker-influenced text behind fmt.Stringer (the scenario
// UserControlled exists to handle).
type userInputStringer struct{ s string }

func (u userInputStringer) String() string { return u.s }

func TestDropVsEscapeSanitizers(t *testing.T) {
	testCases := map[string]struct {
		input      string
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
		"carriage return + line feed": {
			input:      "a\r\nb",
			wantDrop:   "ab",
			wantEscape: "a\\x0d\\x0ab",
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
		"Unicode PARAGRAPH SEPARATOR (U+2029)": {
			input:      "first\u2029second",
			wantDrop:   "firstsecond",
			wantEscape: "first\\u2029second",
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
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.wantDrop, DropControlChars(tc.input), "DropControlChars")
			require.Equal(t, tc.wantEscape, EscapeControlChars(tc.input), "EscapeControlChars")
		})
	}
}

// TestSanitizingLogger documents, for the same set of keyvals passed in,
// what each mode forwards to the underlying logger. Each test case shows
// "input keyvals" -> "drop result" / "escape result".
func TestSanitizingLogger(t *testing.T) {
	dangerousErr := errors.New("parse failed at line\n2: unexpected token\x1b[31m")

	testCases := map[string]struct {
		passedIn   []any
		wantDrop   []any
		wantEscape []any
	}{
		"clean key and value pass through untouched": {
			passedIn:   []any{"msg", "request received"},
			wantDrop:   []any{"msg", "request received"},
			wantEscape: []any{"msg", "request received"},
		},
		"string value with embedded newline": {
			passedIn:   []any{"user_agent", "Mozilla\nlevel=info msg=fake"},
			wantDrop:   []any{"user_agent", "Mozillalevel=info msg=fake"},
			wantEscape: []any{"user_agent", "Mozilla\\x0alevel=info msg=fake"},
		},
		"string value with ANSI escape": {
			passedIn:   []any{"input", "\x1b[31mred\x1b[0m"},
			wantDrop:   []any{"input", "[31mred[0m"},
			wantEscape: []any{"input", "\\x1b[31mred\\x1b[0m"},
		},
		"key with control char is sanitized": {
			passedIn:   []any{"bad\nkey", "v"},
			wantDrop:   []any{"badkey", "v"},
			wantEscape: []any{"bad\\x0akey", "v"},
		},
		"error value sanitized via .Error()": {
			passedIn:   []any{"err", dangerousErr},
			wantDrop:   []any{"err", "parse failed at line2: unexpected token[31m"},
			wantEscape: []any{"err", "parse failed at line\\x0a2: unexpected token\\x1b[31m"},
		},
		"nil error does not panic and passes through": {
			passedIn:   []any{"err", error(nil)},
			wantDrop:   []any{"err", error(nil)},
			wantEscape: []any{"err", error(nil)},
		},
		"non-string, non-error values pass through": {
			passedIn:   []any{"code", 42, "ratio", 3.14, "ok", true},
			wantDrop:   []any{"code", 42, "ratio", 3.14, "ok", true},
			wantEscape: []any{"code", 42, "ratio", 3.14, "ok", true},
		},
		"non-string key (e.g. level.Value) passes through untouched": {
			passedIn:   []any{level.Key(), level.InfoValue()},
			wantDrop:   []any{level.Key(), level.InfoValue()},
			wantEscape: []any{level.Key(), level.InfoValue()},
		},
		"mixed: clean key + dangerous string + dangerous error": {
			passedIn:   []any{"msg", "hi\nthere", "err", dangerousErr},
			wantDrop:   []any{"msg", "hithere", "err", "parse failed at line2: unexpected token[31m"},
			wantEscape: []any{"msg", "hi\\x0athere", "err", "parse failed at line\\x0a2: unexpected token\\x1b[31m"},
		},
		"UserControlled Stringer with embedded newline is sanitized": {
			passedIn:   []any{"req", UserControlled(userInputStringer{"path=/foo\nFAKE"})},
			wantDrop:   []any{"req", "path=/fooFAKE"},
			wantEscape: []any{"req", "path=/foo\\x0aFAKE"},
		},
		"UserControlled non-Stringer struct is rendered via default formatting and sanitized": {
			// fmt.Sprint on a struct without String() emits {field field ...};
			// the dangerous bytes inside the field are still caught.
			passedIn:   []any{"data", UserControlled(struct{ X string }{"ab\nc"})},
			wantDrop:   []any{"data", "{abc}"},
			wantEscape: []any{"data", "{ab\\x0ac}"},
		},
		"UserControlled clean value materialises to its Sprint form": {
			// Even when nothing needs sanitizing, we replace the wrapper
			// with the concrete string so the underlying encoder doesn't
			// re-invoke the fail-closed String() path.
			passedIn:   []any{"req", UserControlled(userInputStringer{"path=/foo"})},
			wantDrop:   []any{"req", "path=/foo"},
			wantEscape: []any{"req", "path=/foo"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			drop := &collectLogger{}
			require.NoError(t, NewSanitizingLogger(drop, DropControlChars).Log(tc.passedIn...))
			require.Equal(t, tc.wantDrop, drop.last())

			esc := &collectLogger{}
			require.NoError(t, NewSanitizingLogger(esc, EscapeControlChars).Log(tc.passedIn...))
			require.Equal(t, tc.wantEscape, esc.last())
		})
	}
}

// TestSanitizingLogger_doesNotMutateCallerSlice verifies we copy on demand
// rather than scribbling into the caller's variadic slice.
func TestSanitizingLogger_doesNotMutateCallerSlice(t *testing.T) {
	kv := []any{"bad\nkey", "evil\nvalue"}
	original := []any{"bad\nkey", "evil\nvalue"}

	inner := &collectLogger{}
	require.NoError(t, NewSanitizingLogger(inner, EscapeControlChars).Log(kv...))

	require.Equal(t, original, kv, "caller's slice must not be mutated")
}

// TestSanitizingLogger_endToEnd_logfmt exercises the full pipeline:
// SanitizingLogger -> go-kit logfmt encoder -> buffer. This shows what an
// operator actually sees on the wire.
func TestSanitizingLogger_endToEnd_logfmt(t *testing.T) {
	testCases := map[string]struct {
		sanitizer  Sanitizer
		keyvals    []any
		wantOnWire string
	}{
		"drop mode strips newline from string value": {
			sanitizer:  DropControlChars,
			keyvals:    []any{"user_agent", "Mozilla\nFAKE"},
			wantOnWire: "user_agent=MozillaFAKE\n",
		},
		"escape mode emits literal backslash-x sequence": {
			sanitizer: EscapeControlChars,
			keyvals:   []any{"user_agent", "Mozilla\nFAKE"},
			// logfmt does not quote on backslash; the value contains no
			// whitespace, =, or quote chars, so it goes through unquoted.
			// What the operator sees: user_agent=Mozilla\x0aFAKE
			wantOnWire: "user_agent=Mozilla\\x0aFAKE\n",
		},
		"key with newline is sanitized (drop)": {
			sanitizer:  DropControlChars,
			keyvals:    []any{"bad\nkey", "v"},
			wantOnWire: "badkey=v\n",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			inner := log.NewLogfmtLogger(buf)
			require.NoError(t, NewSanitizingLogger(inner, tc.sanitizer).Log(tc.keyvals...))
			require.Equal(t, tc.wantOnWire, buf.String())
		})
	}
}

// TestSanitizingLogger_endToEnd_json verifies JSON output stays
// well-formed and the sanitized value appears inside the JSON string.
func TestSanitizingLogger_endToEnd_json(t *testing.T) {
	buf := &bytes.Buffer{}
	inner := log.NewJSONLogger(buf)
	logger := NewSanitizingLogger(inner, EscapeControlChars)

	require.NoError(t, logger.Log("user_agent", "Mozilla\nFAKE"))
	require.JSONEq(t, `{"user_agent":"Mozilla\\x0aFAKE"}`, buf.String())
}

// collectLogger is a go-kit Logger that records the exact keyvals it was
// called with, so tests can assert on the sanitization step in isolation
// (without also testing the underlying encoder).
type collectLogger struct {
	calls [][]any
}

func (c *collectLogger) Log(keyvals ...any) error {
	cp := make([]any, len(keyvals))
	copy(cp, keyvals)
	c.calls = append(c.calls, cp)
	return nil
}

func (c *collectLogger) last() []any {
	return c.calls[len(c.calls)-1]
}

// TestUserControlled_outsideSanitizingLogger documents the fail-closed
// default: if a caller wraps a value with UserControlled but forgets to
// install NewSanitizingLogger, the value's own String / MarshalJSON
// still apply DropControlChars so raw user input cannot leak.
func TestUserControlled_outsideSanitizingLogger(t *testing.T) {
	uc := UserControlled("Mozilla\nlevel=info msg=fake\x1b[31m!")

	t.Run("String drops dangerous chars", func(t *testing.T) {
		require.Equal(t, "Mozillalevel=info msg=fake[31m!", uc.String())
	})

	t.Run("MarshalJSON drops dangerous chars and produces valid JSON", func(t *testing.T) {
		got, err := json.Marshal(uc)
		require.NoError(t, err)
		require.JSONEq(t, `"Mozillalevel=info msg=fake[31m!"`, string(got))
	})

	t.Run("inner Stringer is invoked via fmt.Sprint", func(t *testing.T) {
		uc := UserControlled(userInputStringer{"path=/foo\nFAKE"})
		require.Equal(t, "path=/fooFAKE", uc.String())
	})
}

// TestSanitizerFor verifies that the canonical name constants map to the
// expected Sanitizer functions and that an unknown name returns a useful
// error. Identity is checked via the function pointer (reflect.ValueOf)
// because Sanitizer is a function type and Go does not let us compare
// them with ==.
func TestSanitizerFor(t *testing.T) {
	t.Run("drop", func(t *testing.T) {
		s, err := SanitizerFor(SanitizerNameDrop)
		require.NoError(t, err)
		require.Equal(t, DropControlChars("a\nb"), s("a\nb"))
	})
	t.Run("escape", func(t *testing.T) {
		s, err := SanitizerFor(SanitizerNameEscape)
		require.NoError(t, err)
		require.Equal(t, EscapeControlChars("a\nb"), s("a\nb"))
	})
	t.Run("unknown name returns error listing valid choices", func(t *testing.T) {
		s, err := SanitizerFor("bogus")
		require.Nil(t, s)
		require.Error(t, err)
		require.Contains(t, err.Error(), "bogus")
		require.Contains(t, err.Error(), SanitizerNameDrop)
		require.Contains(t, err.Error(), SanitizerNameEscape)
	})
}

// TestSanitizers_zeroAllocsOnCleanInput is a regression guard for the
// fast path. Both sanitizers must return the input string unchanged with
// no heap allocation when the input contains no dangerous characters,
// because every value passed through NewSanitizingLogger pays this cost.
// If a future change introduces allocation on the clean path (e.g. a
// stray fmt.Sprintf, switching to strings.Map without a fast-path return,
// rewriting needsSanitization with rune iteration), this test fails
// before it ships.
func TestSanitizers_zeroAllocsOnCleanInput(t *testing.T) {
	clean := "ts=2026-05-29 caller=handler.go:42 level=info msg=request_received user_agent=Mozilla/5.0"

	for name, fn := range map[string]func(string) string{
		"DropControlChars":   DropControlChars,
		"EscapeControlChars": EscapeControlChars,
	} {
		t.Run(name, func(t *testing.T) {
			got := testing.AllocsPerRun(100, func() {
				benchSink = fn(clean)
			})
			require.Zero(t, got, "%s must not allocate on clean input", name)
		})
	}
}

// The "clean" sub-benchmarks exercise the fast path — input contains no
// dangerous characters — and should report 0 allocs/op for the pure
// sanitizer functions. The "dirty" sub-benchmarks force allocation by
// triggering the strings.Builder path.

// benchSink prevents the compiler from optimizing away benchmark calls.
var benchSink string

func BenchmarkDropControlChars(b *testing.B) {
	clean := "ts=2026-05-29 caller=handler.go:42 level=info msg=request_received user_agent=Mozilla/5.0"
	dirty := "ts=2026-05-29 \x1b[31mERROR\x1b[0m caller=handler.go:42\nlevel=info msg=oops"

	b.Run("clean", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = DropControlChars(clean)
		}
	})
	b.Run("dirty", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = DropControlChars(dirty)
		}
	})
}

func BenchmarkEscapeControlChars(b *testing.B) {
	clean := "ts=2026-05-29 caller=handler.go:42 level=info msg=request_received user_agent=Mozilla/5.0"
	dirty := "ts=2026-05-29 \x1b[31mERROR\x1b[0m caller=handler.go:42\nlevel=info msg=oops"

	b.Run("clean", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = EscapeControlChars(clean)
		}
	})
	b.Run("dirty", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSink = EscapeControlChars(dirty)
		}
	})
}

// BenchmarkSanitizingLoggerOverhead measures the cost added by wrapping a
// real go-kit logger with NewSanitizingLogger, for each sanitizer mode.
// "plain" calls the bare logfmt encoder; "drop" and "escape" call the
// same encoder through the sanitizing wrapper with DropControlChars and
// EscapeControlChars respectively. The delta between "plain" and either
// wrapped row is that mode's per-call overhead, in isolation.
//
// The keyvals slice is built once and passed with `...` so the
// variadic-to-slice allocation is paid identically by every side and does
// not contaminate the comparison.
func BenchmarkSanitizingLoggerOverhead(b *testing.B) {
	plain := log.NewLogfmtLogger(io.Discard)
	wrappedDrop := NewSanitizingLogger(plain, DropControlChars)
	wrappedEscape := NewSanitizingLogger(plain, EscapeControlChars)
	cleanKV := []any{"ts", "2026-05-29", "level", "info", "msg", "request received", "user_agent", "Mozilla/5.0"}
	dirtyKV := []any{"ts", "2026-05-29", "level", "info", "msg", "request received", "user_agent", "Mozilla\nFAKE"}

	loggers := []struct {
		name   string
		logger log.Logger
	}{
		{"plain", plain},
		{"drop", wrappedDrop},
		{"escape", wrappedEscape},
	}
	inputs := []struct {
		name string
		kv   []any
	}{
		{"clean", cleanKV},
		{"dirty", dirtyKV},
	}

	for _, in := range inputs {
		for _, l := range loggers {
			b.Run(l.name+"/"+in.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = l.logger.Log(in.kv...)
				}
			})
		}
	}
}

// BenchmarkSanitizingLogger measures the per-call overhead of the wrapper
// itself. Note: the variadic-to-slice conversion at the Log() call site
// allocates regardless of what we do, so the "clean" case will not show 0
// allocs/op even though our wrapper avoids copying the keyvals slice.
func BenchmarkSanitizingLogger(b *testing.B) {
	logger := NewSanitizingLogger(log.NewNopLogger(), EscapeControlChars)
	dirty := "Mozilla\nlevel=info msg=fake"

	b.Run("clean", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = logger.Log("ts", "2026-05-29", "level", "info", "msg", "request received", "code", 200)
		}
	})
	b.Run("dirty", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = logger.Log("ts", "2026-05-29", "level", "info", "msg", "request received", "user_agent", dirty)
		}
	})
}
