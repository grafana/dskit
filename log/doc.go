// Package log provides building blocks for constructing go-kit loggers
// used across dskit-based services.
//
// It includes constructors for logfmt and JSON loggers (NewGoKit), a
// Level type wired to a command-line flag, buffered and rate-limited
// loggers, and a global-logger accessor.
//
// For values that may originate from user input (HTTP headers, parsed
// errors, custom Stringers wrapping request data), wrap them at the
// call site with DropUnsafeChars or EscapeUnsafeChars to neutralise
// log injection, terminal escape sequences, and trojan-source attacks
// before they reach the encoder. Only wrapped values are sanitised;
// an unwrapped Stringer carrying user input still reaches the encoder
// verbatim, so review call sites that log non-string, non-error values
// carefully.
package log
