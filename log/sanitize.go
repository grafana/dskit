package log

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

// DropUnsafeChars wraps v so that control and formatting characters
// (newlines, ANSI escape codes, bidi overrides, etc.) are stripped
// from its string representation when logged. Use it for values that
// may carry user input to defend against log injection and terminal-
// escape attacks.
//
// Usage:
//
//	level.Info(logger).Log("user_agent", log.DropUnsafeChars(r.UserAgent()))
func DropUnsafeChars(v any) DroppedUnsafeChars {
	return DroppedUnsafeChars{v: v}
}

// DroppedUnsafeChars is the value type returned by DropUnsafeChars. It
// implements fmt.Stringer and json.Marshaler so it works correctly with
// both logfmt and JSON go-kit encoders.
type DroppedUnsafeChars struct {
	v any
}

// String renders v with unsafe characters removed. Tab is preserved.
// Returns the rendered string unchanged when it contains nothing
// dangerous.
func (d DroppedUnsafeChars) String() string {
	s := render(d.v)
	if !needsSanitization(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if isUnsafe(r) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// MarshalJSON serialises the sanitized form as a JSON string. Without
// this, json.Marshal would emit "{}" for the unexported field.
func (d DroppedUnsafeChars) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// EscapeUnsafeChars wraps v so that control and formatting characters
// (newlines, ANSI escape codes, bidi overrides, etc.) are replaced
// with printable \xNN, \uNNNN, or \UNNNNNNNN sequences in its string
// representation when logged. Use it for values that may carry user
// input to defend against log injection and terminal-escape attacks
// while keeping a readable record of what was sanitized.
//
// Usage:
//
//	level.Info(logger).Log("user_agent", log.EscapeUnsafeChars(r.UserAgent()))
func EscapeUnsafeChars(v any) EscapedUnsafeChars {
	return EscapedUnsafeChars{v: v}
}

// EscapedUnsafeChars is the value type returned by EscapeUnsafeChars.
// It implements fmt.Stringer and json.Marshaler.
type EscapedUnsafeChars struct {
	v any
}

// String renders v with unsafe characters replaced by their printable
// escape form. Tab is preserved. Returns the rendered string unchanged
// when it contains nothing dangerous.
func (e EscapedUnsafeChars) String() string {
	s := render(e.v)
	if !needsSanitization(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if isUnsafe(r) {
			writeEscape(&b, r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// MarshalJSON serialises the sanitized form as a JSON string.
func (e EscapedUnsafeChars) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.String())
}

// render returns v's string form, avoiding a fmt.Sprint allocation
// when v is already a string or an error.
func render(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case error:
		return t.Error()
	}
	return fmt.Sprint(v)
}

// isUnsafe reports whether r is a control or formatting character that
// should be sanitized. Covers Unicode categories Cc (ASCII C0 + DEL +
// C1), Cf (formatting incl. bidi overrides), Zl (line separator U+2028),
// and Zp (paragraph separator U+2029). Tab is treated as safe because
// it has legitimate uses (stack traces, tabular output) and is already
// escaped by logfmt and JSON encoders.
func isUnsafe(r rune) bool {
	if r == '\t' {
		return false
	}
	return unicode.In(r, unicode.Cc, unicode.Cf, unicode.Zl, unicode.Zp)
}

// needsSanitization reports whether s contains any character the
// sanitizers would alter.
func needsSanitization(s string) bool {
	for _, r := range s {
		if isUnsafe(r) {
			return true
		}
	}
	return false
}

// writeEscape appends r's printable escape sequence to b, choosing
// \xNN, \uNNNN, or \UNNNNNNNN according to the rune's width.
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
