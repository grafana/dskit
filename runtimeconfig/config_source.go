package runtimeconfig

import (
	"fmt"
	"slices"
	"strings"
)

// sourceParameter is a per-source parameter parsed from a LoadPath entry.
type sourceParameter string

const (
	parameterOptionalOnStartup              sourceParameter = "optional-on-startup"
	parameterOptionalKeepLastValueOnFailure sourceParameter = "optional-keep-last-value-on-failure"
)

var knownParameters = []sourceParameter{
	parameterOptionalOnStartup,
	parameterOptionalKeepLastValueOnFailure,
}

// sourceParameters are the parameters parsed from one LoadPath entry, in the order
// they were written. An empty list is the default: a failed read aborts the load.
type sourceParameters []sourceParameter

// toleratesFailure reports whether a failed read can be ignored for this parameter.
// initial is true for the load that the Manager performs while it starts.
func (p sourceParameter) toleratesFailure(initial bool) bool {
	switch p {
	case parameterOptionalOnStartup:
		return initial
	case parameterOptionalKeepLastValueOnFailure:
		return true
	default:
		return false
	}
}

// keepsLastValueOnFailure reports whether a tolerated failure keeps the bytes the source last supplied.
func (p sourceParameter) keepsLastValueOnFailure() bool {
	return p == parameterOptionalKeepLastValueOnFailure
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

// keepsLastValueOnFailure reports whether a tolerated failure keeps the bytes the source last supplied.
func (ps sourceParameters) keepsLastValueOnFailure() bool {
	return slices.ContainsFunc(ps, sourceParameter.keepsLastValueOnFailure)
}

// parseConfigSource splits one Config.LoadPath entry into a configSource's path
// and parameters. An entry can end with semicolon-separated parameters, for example:
//
//	/etc/overrides.yaml
//	http://config-server/overrides;optional-on-startup
//	http://config-server/overrides;optional-keep-last-value-on-failure
//
// Known parameters are peeled from the right, so several can be appended as
// ;parameter1;parameter2. Only these exact suffixes are parameters. Anything else after
// a ";" belongs to the path, so a URL parameter such as ;jsessionid=ABC or ;v2
// is left alone. The two parameters above contradict each other, so naming both
// is an error.
func parseConfigSource(entry string) (configSource, error) {
	path := entry
	var parameters sourceParameters
	for {
		rest, parameter, ok := cutParameter(path)
		if !ok {
			break
		}
		if rest == "" {
			return configSource{}, fmt.Errorf("runtime config source %q has no path", entry)
		}
		parameters = append(parameters, parameter)
		path = rest
	}
	// Collected from the right, so restore the order they were written.
	slices.Reverse(parameters)
	if err := checkParameters(entry, parameters); err != nil {
		return configSource{}, err
	}
	return configSource{path: path, parameters: parameters}, nil
}

// checkParameters reports parameters that cannot be combined on one source.
func checkParameters(entry string, parameters sourceParameters) error {
	if len(parameters) > 1 {
		return fmt.Errorf(
			"runtime config source %q has more than one parameter, specify only one of %q and %q",
			entry, parameterOptionalOnStartup, parameterOptionalKeepLastValueOnFailure,
		)
	}
	return nil
}

// cutParameter removes a trailing parameter from entry and reports which it names.
func cutParameter(entry string) (path string, parameter sourceParameter, ok bool) {
	for _, p := range knownParameters {
		s := ";" + string(p)
		if strings.HasSuffix(entry, s) {
			return entry[:len(entry)-len(s)], p, true
		}
	}
	return entry, "", false
}
