package runtimeconfig

import (
	"fmt"
	"strings"
)

// sourceParameter is a per-source option parsed from a LoadPath entry.
type sourceParameter int

const (
	// failureIsFatal aborts the whole load. It is the default.
	failureIsFatal sourceParameter = iota

	// failureToleratedOnStartup lets the Manager start without the source, but aborts
	// every load after that.
	failureToleratedOnStartup

	// failureUsesLastValue keeps the bytes the source last supplied, at startup and after it.
	failureUsesLastValue
)

const (
	optionOptionalOnStartup    = "optional-on-startup"
	optionOptionalUseLastValue = "optional-use-last-value"
)

// source is one entry of Config.LoadPath: where to read from, and how that source behaves.
type source struct {
	path      string
	parameter sourceParameter
}

// tolerates reports whether a failed read can be ignored for this parameter.
// initial is true for the load that the Manager performs while it starts.
func (p sourceParameter) tolerates(initial bool) bool {
	switch p {
	case failureToleratedOnStartup:
		return initial
	case failureUsesLastValue:
		return true
	default:
		return false
	}
}

// keepsLastValue reports whether a tolerated failure keeps the bytes the source last supplied.
func (p sourceParameter) keepsLastValue() bool {
	return p == failureUsesLastValue
}

// parseSource splits one Config.LoadPath entry into a path and a source parameter.
// An entry can end with one option, for example:
//
//	/etc/overrides.yaml
//	http://config-server/overrides;optional-on-startup
//	http://config-server/overrides;optional-use-last-value
//
// Only these exact suffixes are options. Anything else after a ";" belongs to the
// path, so a URL parameter such as ;jsessionid=ABC or ;v2 is left alone.
func parseSource(entry string) (source, error) {
	path, parameter, ok := cutOption(entry)
	if !ok {
		return source{path: entry, parameter: failureIsFatal}, nil
	}
	if path == "" {
		return source{}, fmt.Errorf("runtime config source %q has no path", entry)
	}
	if _, _, again := cutOption(path); again {
		return source{}, fmt.Errorf(
			"runtime config source %q has more than one option, specify only one of %q and %q",
			entry, optionOptionalOnStartup, optionOptionalUseLastValue,
		)
	}
	return source{path: path, parameter: parameter}, nil
}

// cutOption removes a trailing option from entry and reports the parameter it names.
func cutOption(entry string) (path string, parameter sourceParameter, ok bool) {
	if path, ok := strings.CutSuffix(entry, ";"+optionOptionalOnStartup); ok {
		return path, failureToleratedOnStartup, true
	}
	if path, ok := strings.CutSuffix(entry, ";"+optionOptionalUseLastValue); ok {
		return path, failureUsesLastValue, true
	}
	return entry, failureIsFatal, false
}
