package flagext

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// IntString is a type that can be unmarshaled from a string or an integer.
type IntString int

// UnmarshalYAML implements yaml.Unmarshaler.
func (is *IntString) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var i int

	if err := unmarshal(&i); err != nil {
		var s string
		if err = unmarshal(&s); err != nil {
			return fmt.Errorf("IntString unmarshal error: %v", err)
		}

		if i, err = strconv.Atoi(s); err != nil {
			return fmt.Errorf("IntString atoi error: %v", err)
		}
	}

	*is = IntString(i)
	return nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (is *IntString) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var i int

	if err := json.Unmarshal(data, &i); err != nil {
		var s string
		if err = json.Unmarshal(data, &s); err != nil {
			return fmt.Errorf("IntString unmarshal error: %v", err)
		}

		if i, err = strconv.Atoi(s); err != nil {
			return fmt.Errorf("IntString atoi error: %v", err)
		}
	}

	*is = IntString(i)
	return nil
}

// String implements flag.Value.
func (is *IntString) String() string {
	var i int
	if is != nil {
		i = int(*is)
	}
	return fmt.Sprintf("%d", i)
}

// Set implements flag.Value.
func (is *IntString) Set(val string) error {
	if val == "" {
		*is = 0
		return nil
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return fmt.Errorf("IntString set error: %v", err)
	}

	*is = IntString(i)
	return nil
}
