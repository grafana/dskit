package flagext

import "strings"

// StringSliceCSVMulti is a slice of strings that supports both:
// - Multiple flag invocations (values are appended)
// - Comma-separated values (split on commas)
// It implements flag.Value
type StringSliceCSVMulti []string

// String implements flag.Value
func (v StringSliceCSVMulti) String() string {
	return strings.Join(v, ",")
}

// Set implements flag.Value
func (v *StringSliceCSVMulti) Set(s string) error {
	if len(s) == 0 {
		return nil
	}
	*v = append(*v, strings.Split(s, ",")...)
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (v *StringSliceCSVMulti) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Try to unmarshal as string first (CSV format)
	var s string
	if err := unmarshal(&s); err == nil {
		// For YAML, we replace rather than append
		if len(s) == 0 {
			*v = nil
			return nil
		}
		*v = strings.Split(s, ",")
		return nil
	}

	// If that fails, try to unmarshal as a slice (list format) for backward compatibility
	var slice []string
	if err := unmarshal(&slice); err != nil {
		return err
	}
	*v = slice
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (v StringSliceCSVMulti) MarshalYAML() (interface{}, error) {
	return v.String(), nil
}
