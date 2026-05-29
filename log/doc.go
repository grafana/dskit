// Package log provides building blocks for constructing go-kit loggers used
// across dskit-based services.
//
// The package exposes:
//
//   - Logger constructors (NewGoKit, NewGoKitWithLevel, NewGoKitWithWriter)
//     that produce a standard go-kit logger in either logfmt or JSON format.
//   - A Level type usable as a flag.Value to wire `-log.level` from the
//     command line.
//   - A buffered logger (BufferedLogger) and a rate-limited logger
//     (NewRateLimitedLogger) for high-volume log paths.
//   - A global logger accessor (Global / SetGlobal) for code paths that
//     cannot easily plumb a logger.
//
// # Sanitizing user-influenced log values
//
// Anywhere a log value can be controlled by external input (HTTP headers,
// request bodies, parsed error messages, etc.) an attacker can embed ASCII
// or Unicode control characters that:
//
//   - inject fake log lines (newline + a forged level/key prefix),
//   - manipulate the terminal of an operator tailing logs (ANSI escape
//     sequences),
//   - confuse log parsers (U+2028, U+2029, NEL),
//   - hide source via bidi-override characters (the "trojan source" attack).
//
// To defend against this, wrap the underlying logger with
// NewSanitizingLogger, choosing between DropControlChars (removes dangerous
// characters; safer) and EscapeControlChars (replaces with \xNN / \uNNNN
// sequences; preserves forensic information). The sanitizer is intentionally
// not wired into NewGoKit; consumers opt in by composing the wrapper:
//
//	logger := log.NewGoKitWithLevel(lvl, format)
//	logger = log.NewSanitizingLogger(logger, log.EscapeControlChars) // or DropControlChars
//
// The wrapper automatically sanitizes string- and error-typed values.
// For other types whose contents may originate from user input (custom
// fmt.Stringers wrapping request data, structs with user-controlled
// fields, etc.), wrap the value at the call site with UserControlled so
// the SanitizingLogger knows to materialise and sanitize it.
//
// The clean-input path is allocation-free and runs at memory-throughput
// speed; sanitization only allocates when a value actually contains
// dangerous characters.
package log
