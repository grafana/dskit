package log

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	"github.com/go-kit/log"
)

// Sanitizer transforms a string by removing or escaping characters that
// could be used to inject log lines, fake terminal output, confuse log
// parsers, or hide source via bidi overrides. Two implementations are
// provided: DropControlChars and EscapeControlChars. Callers select one
// (typically driven by their own configuration flag) and pass it to
// NewSanitizingLogger.
type Sanitizer func(string) string

// Canonical names for the built-in sanitizers, intended for use as
// command-line flag or YAML configuration values. Downstream consumers
// should prefer these constants over hard-coding string literals so that
// multiple services converge on the same spelling.
const (
	SanitizerNameDrop   = "drop"
	SanitizerNameEscape = "escape"
)

// SanitizerFor returns the Sanitizer matching name, or an error listing
// the valid choices if name is unrecognized. It is intended for wiring
// up a configuration flag, for example:
//
//	var mode string
//	flag.StringVar(&mode, "log.sanitization", log.SanitizerNameDrop,
//	    "Sanitizer for user-controlled log values: drop or escape.")
//	// ... after flag.Parse() ...
//	s, err := log.SanitizerFor(mode)
//	if err != nil {
//	    return err
//	}
//	logger = log.NewSanitizingLogger(logger, s)
func SanitizerFor(name string) (Sanitizer, error) {
	switch name {
	case SanitizerNameDrop:
		return DropControlChars, nil
	case SanitizerNameEscape:
		return EscapeControlChars, nil
	default:
		return nil, fmt.Errorf("unknown sanitizer %q (valid: %s, %s)", name, SanitizerNameDrop, SanitizerNameEscape)
	}
}

// DropControlChars removes ASCII and Unicode control/formatting characters
// from s. The tab character is preserved because it has legitimate uses
// (stack traces, tabular output) and is escaped by both logfmt and JSON
// encoders.
//
// Returns s unchanged (no allocation) when it contains no dangerous chars.
func DropControlChars(s string) string {
	if !needsSanitization(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if isDangerous(r) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// EscapeControlChars replaces ASCII and Unicode control/formatting
// characters in s with \xNN, \uNNNN, or \UNNNNNNNN sequences. Compared to
// DropControlChars this preserves visible length and makes it obvious to
// an operator that sanitization happened.
//
// Returns s unchanged (no allocation) when it contains no dangerous chars.
func EscapeControlChars(s string) string {
	if !needsSanitization(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if isDangerous(r) {
			writeEscape(&b, r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// isDangerous reports whether r is a control or formatting character that
// should be sanitized. Covers Unicode categories Cc (ASCII C0 + DEL + C1),
// Cf (formatting incl. bidi overrides), Zl (line separator U+2028), and
// Zp (paragraph separator U+2029). Tab is treated as safe.
func isDangerous(r rune) bool {
	if r == '\t' {
		return false
	}
	return unicode.In(r, unicode.Cc, unicode.Cf, unicode.Zl, unicode.Zp)
}

// needsSanitization reports whether s contains any character the
// sanitizers would alter. This is the hot fast-path: for clean inputs
// (the overwhelmingly common case in production logs) it must return
// false as cheaply as possible, because the sanitizers short-circuit
// to "return s unchanged" on a false result and that path runs on
// every value passed through NewSanitizingLogger.
//
// A naive `for _, r := range s { isDangerous(r) }` costs ~9 ns/byte
// even on plain ASCII, because each rune triggers utf8.DecodeRune plus
// four binary-search lookups inside unicode.In(r, Cc, Cf, Zl, Zp).
// Since the vast majority of log content is ASCII, we scan bytes
// instead: printable ASCII and tab are safe, any other ASCII byte
// (C0 controls or DEL) is dangerous, and only when we hit a high byte
// (>= 0x80) do we fall back to rune decoding to check whether the
// multi-byte sequence belongs to Cf / Zl / Zp (bidi overrides,
// U+2028, U+2029, etc.). This brings the clean ASCII path close to
// memcmp speed (~1 ns/byte).
func needsSanitization(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\t':
			// Safe by design (see isDangerous).
		case c >= 0x20 && c < 0x7f:
			// Printable ASCII.
		case c < 0x80:
			// ASCII C0 control or DEL — always dangerous.
			return true
		default:
			// Non-ASCII: hand the remainder to the rune-level check.
			// Anything before s[i] was confirmed safe by the byte scan.
			return needsSanitizationRuneLevel(s[i:])
		}
	}
	return false
}

func needsSanitizationRuneLevel(s string) bool {
	for _, r := range s {
		if isDangerous(r) {
			return true
		}
	}
	return false
}

func writeEscape(b *strings.Builder, r rune) {
	switch {
	case r < 0x100:
		fmt.Fprintf(b, `\x%02x`, r)
	case r < 0x10000:
		fmt.Fprintf(b, `\u%04x`, r)
	default:
		fmt.Fprintf(b, `\U%08x`, r)
	}
}

// UserControlled marks v as a value that may carry attacker-influenced
// content (a custom Stringer wrapping request data, a struct whose
// fields came from user input, etc.). When the logger is wrapped with
// NewSanitizingLogger, the wrapper recognises UserControlledValue by
// type, calls fmt.Sprint on the inner value, and sanitizes the result
// with the configured Sanitizer.
//
// This exists because the SanitizingLogger normally only sanitizes
// string- and error-typed values; anything else (including arbitrary
// fmt.Stringer implementations) passes through untouched on the
// assumption that it cannot carry user input. Wrap with UserControlled
// to opt a non-string, non-error value into sanitization explicitly.
//
// Fail-closed default: if a wrapped value is logged through a logger
// that is NOT a SanitizingLogger, the value's own String / MarshalJSON
// still apply DropControlChars so raw user input cannot leak by accident.
//
// Usage:
//
//	req := ParsedRequest{Path: r.URL.Path}
//	level.Info(logger).Log("msg", "request rejected", "req", log.UserControlled(req))
func UserControlled(v any) UserControlledValue {
	return UserControlledValue{v: v}
}

// UserControlledValue is the return type of UserControlled. Exported so
// that a type assertion inside SanitizingLogger can recognise it. The
// inner value is unexported; access is only through the configured
// Sanitizer (inside SanitizingLogger) or via String / MarshalJSON (the
// fail-closed default outside).
type UserControlledValue struct {
	v any
}

// String renders the inner value through DropControlChars. This is what
// logfmt encoders see via fmt.Sprint when the wrapper is used outside a
// SanitizingLogger; the safe-by-default behaviour ensures raw user input
// cannot leak even if the caller forgets to install the wrapper.
func (u UserControlledValue) String() string {
	return DropControlChars(fmt.Sprint(u.v))
}

// MarshalJSON renders the inner value (sanitized) as a JSON string.
// Without this method, json.Marshal would serialize UserControlledValue
// as "{}" because the inner field is unexported.
func (u UserControlledValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// NewSanitizingLogger wraps next so that every log key, every string-
// or error-typed value, and every UserControlledValue passes through
// the supplied Sanitizer before reaching the underlying logger. Other
// types (numbers, timestamps, internal Stringers, etc.) pass through
// untouched on the assumption that they cannot carry attacker-controlled
// content; if that assumption is wrong for a specific value, wrap it
// with UserControlled at the call site.
//
// Pass DropControlChars or EscapeControlChars depending on your
// configuration.
func NewSanitizingLogger(next log.Logger, s Sanitizer) log.Logger {
	return sanitizingLogger{next: next, sanitize: s}
}

type sanitizingLogger struct {
	next     log.Logger
	sanitize Sanitizer
}

func (l sanitizingLogger) Log(keyvals ...any) error {
	out := keyvals
	copied := false
	ensureCopy := func() {
		if copied {
			return
		}
		out = make([]any, len(keyvals))
		copy(out, keyvals)
		copied = true
	}

	for i := 0; i+1 < len(keyvals); i += 2 {
		if v, replace := l.maybeSanitize(keyvals[i]); replace {
			ensureCopy()
			out[i] = v
		}
		if v, replace := l.maybeSanitize(keyvals[i+1]); replace {
			ensureCopy()
			out[i+1] = v
		}
	}
	return l.next.Log(out...)
}

// maybeSanitize inspects a single keyval element and returns (sanitized,
// true) when it should replace the original, or (nil, false) when it
// should pass through. Strings are sanitized if they contain dangerous
// chars; errors are sanitized if their .Error() does; UserControlledValue
// values are always materialised via fmt.Sprint and sanitized, because
// the wrapper at the call site is the explicit "this came from outside"
// signal — we always have to look at the bytes.
func (l sanitizingLogger) maybeSanitize(v any) (any, bool) {
	switch t := v.(type) {
	case string:
		if !needsSanitization(t) {
			return nil, false
		}
		return l.sanitize(t), true
	case error:
		msg := t.Error()
		if !needsSanitization(msg) {
			return nil, false
		}
		return l.sanitize(msg), true
	case UserControlledValue:
		// l.sanitize has its own clean-input fast path (returns the
		// input unchanged when nothing is dangerous), so we don't
		// duplicate the needsSanitization check here.
		return l.sanitize(fmt.Sprint(t.v)), true
	}
	return nil, false
}
