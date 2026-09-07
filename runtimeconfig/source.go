package runtimeconfig

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
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

func policyForOption(option string) (failurePolicy, bool) {
	switch option {
	case optionOptionalOnStartup:
		return failureToleratedOnStartup, true
	case optionOptionalUseLastValue:
		return failureUsesLastValue, true
	default:
		return 0, false
	}
}

// parseSource splits one Config.LoadPath entry into a path and a failure policy.
// An entry can end with a recognized option, for example:
//
//	/etc/overrides.yaml
//	http://config-server/overrides;optional-on-startup
//	http://config-server/overrides;optional-use-last-value
//
// Options are peeled from the right only when recognized, so a URL path parameter
// such as ;jsessionid=ABC stays part of the path. The two options contradict each
// other, so naming both is an error.
func parseSource(entry string) (source, error) {
	path, options, err := splitSourceOptions(entry)
	if err != nil {
		return source{}, err
	}
	if path == "" {
		return source{}, fmt.Errorf("runtime config source %q has no path", entry)
	}
	switch len(options) {
	case 0:
		return source{path: path, policy: failureIsFatal}, nil
	case 1:
		policy, _ := policyForOption(options[0])
		return source{path: path, policy: policy}, nil
	default:
		return source{}, fmt.Errorf(
			"runtime config source %q has multiple options %q; specify only one of %q and %q",
			entry, strings.Join(options, ";"), optionOptionalOnStartup, optionOptionalUseLastValue,
		)
	}
}

// splitSourceOptions peels recognized options off the end of entry. Each
// option is a semicolon-prefixed token, like JDBC URL parameters.
func splitSourceOptions(entry string) (path string, options []string, err error) {
	path = entry
	for {
		semi := strings.LastIndex(path, ";")
		if semi < 0 {
			break
		}
		option := path[semi+1:]
		if _, ok := policyForOption(option); ok {
			options = append(options, option)
			path = path[:semi]
			continue
		}
		if option == "" {
			return "", nil, fmt.Errorf("runtime config source %q has an empty option", entry)
		}
		// An unrecognized token shaped like an option name is a typo. Anything else
		// (";jsessionid=ABC") stays part of the path.
		if isOptionName(option) {
			return "", nil, fmt.Errorf(
				"runtime config source %q has unknown option %q, supported options are %q and %q",
				entry, option, optionOptionalOnStartup, optionOptionalUseLastValue,
			)
		}
		break
	}
	// Collected from the right, so restore input order.
	slices.Reverse(options)
	return path, options, nil
}

// isOptionName reports whether s could be one of our option names: lowercase
// letters, digits, and hyphens, starting with a letter.
func isOptionName(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case unicode.IsLower(r):
		case i > 0 && (unicode.IsDigit(r) || r == '-'):
		default:
			return false
		}
	}
	return true
}
