// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/format.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

import (
	"flag"
)

// Format is a settable identifier for the output format of logs
type Format struct {
	s string
}

// RegisterFlags adds the log format flag to the provided flagset.
func (f *Format) RegisterFlags(fs *flag.FlagSet) {
	_ = f.Set("logfmt")
	fs.Var(f, "log.format", "Output log messages in the given format. Valid formats: [logfmt, json]")
}

func (f Format) String() string {
	return f.s
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (f *Format) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var format string
	if err := unmarshal(&format); err != nil {
		return err
	}
	return f.Set(format)
}

// MarshalYAML implements yaml.Marshaler.
func (f Format) MarshalYAML() (interface{}, error) {
	return f.String(), nil
}

// Set updates the value of the output format.  Implements flag.Value
func (f *Format) Set(s string) error {
	f.s = s
	return nil
}
