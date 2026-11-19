package flagext

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
)

// StringSliceCSVQuoted is a slice of strings that is parsed from a comma-separated string
// following the quoting rules of encoding/csv, so that values can contain commas or quotes.
type StringSliceCSVQuoted []string

// String implements flag.Value
func (v StringSliceCSVQuoted) String() string {
	var sb strings.Builder
	w := csv.NewWriter(&sb)
	_ = w.Write(v)
	w.Flush()
	return strings.TrimSuffix(sb.String(), "\n")
}

// Set implements flag.Value
func (v *StringSliceCSVQuoted) Set(s string) error {
	if len(s) == 0 {
		*v = nil
		return nil
	}
	r := csv.NewReader(strings.NewReader(s))
	r.FieldsPerRecord = -1
	record, err := r.Read()
	if err != nil {
		return fmt.Errorf("invalid comma-separated value %q: %w", s, err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("invalid comma-separated value %q: %w", s, err)
		}
		return fmt.Errorf("invalid comma-separated value %q: unquoted newline", s)
	}
	*v = record
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (v *StringSliceCSVQuoted) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	return v.Set(s)
}

// MarshalYAML implements yaml.Marshaler.
func (v StringSliceCSVQuoted) MarshalYAML() (interface{}, error) {
	return v.String(), nil
}
