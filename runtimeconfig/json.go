package runtimeconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

func unmarshalYAMLCompatibleJSON(data []byte) (map[string]any, error) {
	if err := validateYAMLCompatibleJSON(data); err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var config map[string]any
	if err := dec.Decode(&config); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(dec); err != nil {
		return nil, err
	}

	normalized, err := normalizeJSONNumbers(config)
	if err != nil {
		return nil, err
	}
	return normalized.(map[string]any), nil
}

const (
	inlineJSONObjectKeys = 8
	maxJSONNestingDepth  = 10000
)

type yamlCompatibleJSONScanner struct {
	data []byte
}

type jsonStringPosition struct {
	end          int
	contentStart int
	contentEnd   int
	hasEscape    bool
}

type jsonObjectKeys struct {
	inline   [inlineJSONObjectKeys]string
	count    int
	overflow map[string]struct{}
}

func validateYAMLCompatibleJSON(data []byte) error {
	// Scalar grammar is left to encoding/json below. This pass only checks the
	// structure and string semantics needed to decide whether YAML would agree.
	scanner := yamlCompatibleJSONScanner{data: data}
	pos := scanner.skipWhitespace(0)
	if pos >= len(data) || data[pos] != '{' {
		return fmt.Errorf("JSON configuration must be an object")
	}

	pos, err := scanner.scanObject(pos, 1)
	if err != nil {
		return err
	}
	if pos = scanner.skipWhitespace(pos); pos != len(data) {
		return fmt.Errorf("multiple JSON values")
	}
	return nil
}

func (s yamlCompatibleJSONScanner) scanValue(pos, depth int) (int, error) {
	pos = s.skipWhitespace(pos)
	if pos >= len(s.data) {
		return pos, io.ErrUnexpectedEOF
	}

	switch s.data[pos] {
	case '{':
		return s.scanObject(pos, depth+1)
	case '[':
		return s.scanArray(pos, depth+1)
	case '"':
		value, err := s.scanString(pos)
		return value.end, err
	default:
		return s.scanScalar(pos)
	}
}

func (s yamlCompatibleJSONScanner) scanObject(pos, depth int) (int, error) {
	if depth > maxJSONNestingDepth {
		return pos, fmt.Errorf("JSON nesting depth exceeds %d", maxJSONNestingDepth)
	}

	pos = s.skipWhitespace(pos + 1)
	if pos < len(s.data) && s.data[pos] == '}' {
		return pos + 1, nil
	}

	var keys jsonObjectKeys
	for {
		if pos >= len(s.data) || s.data[pos] != '"' {
			return pos, fmt.Errorf("expected JSON object key")
		}

		keyPosition, err := s.scanString(pos)
		if err != nil {
			return pos, err
		}
		key, err := s.decodeKey(pos, keyPosition)
		if err != nil {
			return pos, err
		}
		if keys.add(key) {
			return pos, fmt.Errorf("duplicate JSON object key %q", key)
		}

		pos = s.skipWhitespace(keyPosition.end)
		if pos >= len(s.data) || s.data[pos] != ':' {
			return pos, fmt.Errorf("expected colon after JSON object key")
		}
		pos, err = s.scanValue(pos+1, depth)
		if err != nil {
			return pos, err
		}

		pos = s.skipWhitespace(pos)
		if pos >= len(s.data) {
			return pos, io.ErrUnexpectedEOF
		}
		switch s.data[pos] {
		case '}':
			return pos + 1, nil
		case ',':
			pos = s.skipWhitespace(pos + 1)
		default:
			return pos, fmt.Errorf("expected comma or end of JSON object")
		}
	}
}

func (s yamlCompatibleJSONScanner) scanArray(pos, depth int) (int, error) {
	if depth > maxJSONNestingDepth {
		return pos, fmt.Errorf("JSON nesting depth exceeds %d", maxJSONNestingDepth)
	}

	pos = s.skipWhitespace(pos + 1)
	if pos < len(s.data) && s.data[pos] == ']' {
		return pos + 1, nil
	}

	for {
		var err error
		pos, err = s.scanValue(pos, depth)
		if err != nil {
			return pos, err
		}

		pos = s.skipWhitespace(pos)
		if pos >= len(s.data) {
			return pos, io.ErrUnexpectedEOF
		}
		switch s.data[pos] {
		case ']':
			return pos + 1, nil
		case ',':
			pos = s.skipWhitespace(pos + 1)
		default:
			return pos, fmt.Errorf("expected comma or end of JSON array")
		}
	}
}

func (s yamlCompatibleJSONScanner) scanString(pos int) (jsonStringPosition, error) {
	value := jsonStringPosition{contentStart: pos + 1}
	for i := pos + 1; i < len(s.data); {
		switch b := s.data[i]; {
		case b == '"':
			value.end = i + 1
			value.contentEnd = i
			return value, nil

		case b == '\\':
			value.hasEscape = true
			if i+1 >= len(s.data) {
				return value, io.ErrUnexpectedEOF
			}
			switch s.data[i+1] {
			case '"', '\\', 'b', 'f', 'n', 'r', 't':
				i += 2
			case '/':
				return value, fmt.Errorf("escaped solidus is not accepted by YAML")
			case 'u':
				if i+6 > len(s.data) {
					return value, io.ErrUnexpectedEOF
				}
				codePoint, err := strconv.ParseUint(string(s.data[i+2:i+6]), 16, 16)
				if err != nil {
					return value, fmt.Errorf("invalid Unicode escape")
				}
				if codePoint >= 0xd800 && codePoint <= 0xdfff {
					return value, fmt.Errorf("UTF-16 surrogate escape is not accepted by YAML")
				}
				i += 6
			default:
				return value, fmt.Errorf("invalid JSON escape")
			}

		case b < 0x20:
			return value, fmt.Errorf("unescaped control character in JSON string")

		case b == 0x7f:
			return value, fmt.Errorf("raw Unicode character U+007F is not YAML-compatible")

		case b < utf8.RuneSelf:
			i++

		default:
			r, size := utf8.DecodeRune(s.data[i:])
			if r == utf8.RuneError && size == 1 {
				return value, fmt.Errorf("invalid UTF-8")
			}
			if !yamlAllowsRawJSONRune(r) {
				return value, fmt.Errorf("raw Unicode character U+%04X is not YAML-compatible", r)
			}
			i += size
		}
	}
	return value, io.ErrUnexpectedEOF
}

func (s yamlCompatibleJSONScanner) scanScalar(pos int) (int, error) {
	start := pos
	for pos < len(s.data) {
		switch s.data[pos] {
		case ' ', '\t', '\r', '\n', ',', '}', ']':
			if pos == start {
				return pos, fmt.Errorf("empty JSON value")
			}
			return pos, nil
		default:
			pos++
		}
	}
	if pos == start {
		return pos, fmt.Errorf("empty JSON value")
	}
	return pos, nil
}

func (s yamlCompatibleJSONScanner) skipWhitespace(pos int) int {
	for pos < len(s.data) {
		switch s.data[pos] {
		case ' ', '\t', '\r', '\n':
			pos++
		default:
			return pos
		}
	}
	return pos
}

func (s yamlCompatibleJSONScanner) decodeKey(start int, position jsonStringPosition) (string, error) {
	if !position.hasEscape {
		return string(s.data[position.contentStart:position.contentEnd]), nil
	}
	return strconv.Unquote(string(s.data[start:position.end]))
}

func (k *jsonObjectKeys) add(key string) bool {
	if k.overflow != nil {
		if _, exists := k.overflow[key]; exists {
			return true
		}
		k.overflow[key] = struct{}{}
		return false
	}

	for i := 0; i < k.count; i++ {
		if k.inline[i] == key {
			return true
		}
	}
	if k.count < len(k.inline) {
		k.inline[k.count] = key
		k.count++
		return false
	}

	k.overflow = make(map[string]struct{}, len(k.inline)+1)
	for _, existing := range k.inline {
		k.overflow[existing] = struct{}{}
	}
	k.overflow[key] = struct{}{}
	return false
}

func yamlAllowsRawJSONRune(r rune) bool {
	// YAML rejects C1 controls and BMP noncharacters, and folds these three
	// line-break runes. Their escaped forms do not pass through the YAML reader
	// and can remain on the JSON fast path.
	switch {
	case r >= 0x7f && r <= 0x9f:
		return false
	case r == '\u2028' || r == '\u2029':
		return false
	case r == '\ufffe' || r == '\uffff':
		return false
	default:
		return true
	}
}

func requireJSONEOF(dec *json.Decoder) error {
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func normalizeJSONNumbers(value any) (any, error) {
	switch value := value.(type) {
	case json.Number:
		return yamlCompatibleJSONNumber(value)

	case map[string]any:
		for key, child := range value {
			normalized, err := normalizeJSONNumbers(child)
			if err != nil {
				return nil, err
			}
			value[key] = normalized
		}
		return value, nil

	case []any:
		for i, child := range value {
			normalized, err := normalizeJSONNumbers(child)
			if err != nil {
				return nil, err
			}
			value[i] = normalized
		}
		return value, nil

	default:
		return value, nil
	}
}

func yamlCompatibleJSONNumber(number json.Number) (any, error) {
	value := number.String()
	if strings.ContainsAny(value, ".eE") {
		number, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		return number, nil
	}

	if number, err := strconv.ParseInt(value, 10, 64); err == nil {
		if int64(int(number)) == number {
			return int(number), nil
		}
		return number, nil
	}
	if number, err := strconv.ParseUint(value, 10, 64); err == nil {
		return number, nil
	}
	if number, err := strconv.ParseFloat(value, 64); err == nil {
		return number, nil
	}
	return nil, fmt.Errorf("cannot decode JSON number %q with YAML semantics", value)
}
