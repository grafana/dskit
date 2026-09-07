package runtimeconfig

import (
	"fmt"
	"slices"
	"strings"
)

// sourceParameter is a per-source option parsed from a LoadPath entry.
type sourceParameter int

const (
	// failureIsFatal aborts the whole load. It is the default when a source has
	// no parameters.
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

// sourceParameters are the options parsed from one LoadPath entry, in the order
// they were written. An empty list is the default: a failed read aborts the load.
type sourceParameters []sourceParameter

// source is one entry of Config.LoadPath: where to read from, and how that source behaves.
type source struct {
	path       string
	parameters sourceParameters
}

// toleratesFailure reports whether a failed read can be ignored for this parameter.
// initial is true for the load that the Manager performs while it starts.
func (p sourceParameter) toleratesFailure(initial bool) bool {
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

// toleratesFailure reports whether a failed read can be ignored given these parameters.
func (ps sourceParameters) toleratesFailure(initial bool) bool {
	for _, p := range ps {
		if p.toleratesFailure(initial) {
			return true
		}
	}
	return false
}

// keepsLastValue reports whether a tolerated failure keeps the bytes the source last supplied.
func (ps sourceParameters) keepsLastValue() bool {
	return slices.ContainsFunc(ps, sourceParameter.keepsLastValue)
}

// parseSource splits one Config.LoadPath entry into a path and source parameters.
// An entry can end with semicolon-separated options, for example:
//
//	/etc/overrides.yaml
//	http://config-server/overrides;optional-on-startup
//	http://config-server/overrides;optional-use-last-value
//
// Known options are peeled from the right, so several can be appended as
// ;option1;option2. Only these exact suffixes are options. Anything else after
// a ";" belongs to the path, so a URL parameter such as ;jsessionid=ABC or ;v2
// is left alone. The two options above contradict each other, so naming both
// is an error.
func parseSource(entry string) (source, error) {
	path := entry
	var parameters sourceParameters
	for {
		rest, parameter, ok := cutOption(path)
		if !ok {
			break
		}
		if rest == "" {
			return source{}, fmt.Errorf("runtime config source %q has no path", entry)
		}
		parameters = append(parameters, parameter)
		path = rest
	}
	// Collected from the right, so restore the order they were written.
	slices.Reverse(parameters)
	if err := checkParameters(entry, parameters); err != nil {
		return source{}, err
	}
	return source{path: path, parameters: parameters}, nil
}

// checkParameters reports options that cannot be combined on one source.
func checkParameters(entry string, parameters sourceParameters) error {
	var failureOptions int
	for _, p := range parameters {
		switch p {
		case failureToleratedOnStartup, failureUsesLastValue:
			failureOptions++
		}
	}
	if failureOptions > 1 {
		return fmt.Errorf(
			"runtime config source %q has more than one option, specify only one of %q and %q",
			entry, optionOptionalOnStartup, optionOptionalUseLastValue,
		)
	}
	return nil
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
