package runtimeconfig

import (
	"fmt"
	"strings"
)

// failurePolicy says what the Manager does when a source cannot be read.
type failurePolicy int

const (
	// failureIsFatal aborts the whole load. This is the default, and it applies
	// to every source that does not opt out.
	failureIsFatal failurePolicy = iota

	// failureToleratedOnStartup lets the Manager start when the source cannot be
	// read, but aborts the load on every attempt after that.
	failureToleratedOnStartup

	// failureUsesLastValue keeps the bytes the source supplied last time it was
	// read successfully, at startup and after it.
	failureUsesLastValue
)

const (
	optionOptionalOnStartup    = "optional-on-startup"
	optionOptionalUseLastValue = "optional-use-last-value"
)

// source is one entry of Config.LoadPath: where to read the config from, and
// what to do when that read fails.
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

// keepsLastValue reports whether a tolerated failure keeps the bytes the source
// supplied last time. A source that has never been read successfully supplies
// nothing, because there is no last value to keep.
func (p failurePolicy) keepsLastValue() bool {
	return p == failureUsesLastValue
}

// parseSource splits one Config.LoadPath entry into a path and a failure
// policy. An entry can end with one option in square brackets, for example:
//
//	/etc/overrides.yaml
//	http://config-server/overrides[optional-on-startup]
//	http://config-server/overrides[optional-use-last-value]
//
// Only a trailing "[...]" is an option, so an IPv6 URL such as
// http://[::1]:8080/overrides keeps its brackets.
func parseSource(entry string) (source, error) {
	if !strings.HasSuffix(entry, "]") {
		return source{path: entry, policy: failureIsFatal}, nil
	}

	open := strings.LastIndex(entry, "[")
	if open < 0 {
		return source{}, fmt.Errorf("runtime config source %q ends with %q but has no matching %q", entry, "]", "[")
	}

	path := entry[:open]
	option := entry[open+1 : len(entry)-1]

	switch option {
	case optionOptionalOnStartup:
		return source{path: path, policy: failureToleratedOnStartup}, nil
	case optionOptionalUseLastValue:
		return source{path: path, policy: failureUsesLastValue}, nil
	default:
		return source{}, fmt.Errorf(
			"runtime config source %q has unknown option %q, supported options are %q and %q",
			entry, option, optionOptionalOnStartup, optionOptionalUseLastValue,
		)
	}
}
