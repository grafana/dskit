package runtimeconfig

import (
	"fmt"
	"strings"
)

// failurePolicy says what the Manager does when a source cannot be read.
type failurePolicy int

const (
	// failureIsFatal aborts the whole load. It is the default.
	failureIsFatal failurePolicy = iota

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

// source is one entry of Config.LoadPath: where to read from, and what to do when that fails.
type source struct {
	path   string
	policy failurePolicy
}

// tolerates reports whether a failed read can be ignored under this policy.
// initial is true for the load that the Manager performs while it starts.
func (p failurePolicy) tolerates(initial bool) bool {
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
func (p failurePolicy) keepsLastValue() bool {
	return p == failureUsesLastValue
}

// parseSource splits one Config.LoadPath entry into a path and a failure policy.
// An entry can end with one option, for example:
//
//	/etc/overrides.yaml
//	http://config-server/overrides;optional-on-startup
//	http://config-server/overrides;optional-use-last-value
//
// Only these exact suffixes are options. Anything else after a ";" belongs to the
// path, so a URL parameter such as ;jsessionid=ABC or ;v2 is left alone.
func parseSource(entry string) (source, error) {
	path, policy, ok := cutOption(entry)
	if !ok {
		return source{path: entry, policy: failureIsFatal}, nil
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
	return source{path: path, policy: policy}, nil
}

// cutOption removes a trailing option from entry and reports the policy it names.
func cutOption(entry string) (path string, policy failurePolicy, ok bool) {
	if path, ok := strings.CutSuffix(entry, ";"+optionOptionalOnStartup); ok {
		return path, failureToleratedOnStartup, true
	}
	if path, ok := strings.CutSuffix(entry, ";"+optionOptionalUseLastValue); ok {
		return path, failureUsesLastValue, true
	}
	return entry, failureIsFatal, false
}
