package ring

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyOptionalErrorFilters(t *testing.T) {
	err := mockError{
		isClientErr: true,
		message:     "this is a foo error",
	}

	var (
		isClientErrorOption OptionalErrorFilter = func(err error) bool {
			if mockErr, ok := err.(mockError); ok {
				return mockErr.isClientErr
			}
			return false
		}
		isServerErrorOption OptionalErrorFilter = func(err error) bool {
			if mockErr, ok := err.(mockError); ok {
				return !mockErr.isClientErr
			}
			return false
		}
		messageContainsFooOption OptionalErrorFilter = func(err error) bool {
			return strings.Contains(err.Error(), "foo")
		}
		messageContainsBarOption OptionalErrorFilter = func(err error) bool {
			return strings.Contains(err.Error(), "bar")
		}
	)

	testCases := map[string]struct {
		optionalFilters []OptionalErrorFilter
		expectedOutcome bool
	}{
		"no filter returns false": {
			expectedOutcome: false,
		},
		"isClientErrorOption on a client error gives true": {
			optionalFilters: []OptionalErrorFilter{isClientErrorOption},
			expectedOutcome: true,
		},
		"messageContainsFooOption on an error containing foo gives true": {
			optionalFilters: []OptionalErrorFilter{messageContainsFooOption},
			expectedOutcome: true,
		},
		"isServerErrorOption on a client error gives false": {
			optionalFilters: []OptionalErrorFilter{isServerErrorOption},
			expectedOutcome: false,
		},
		"messageContainsBarOption on an error not containing bar gives false": {
			optionalFilters: []OptionalErrorFilter{messageContainsBarOption},
			expectedOutcome: false,
		},
		"isClientErrorOption and messageContainsBar on a client error not containing bar gives true": {
			optionalFilters: []OptionalErrorFilter{isClientErrorOption, messageContainsBarOption},
			expectedOutcome: true,
		},
		"isServerErrorOption and messageContainsBar on a client error not containing bar gives false": {
			optionalFilters: []OptionalErrorFilter{isServerErrorOption, messageContainsBarOption},
			expectedOutcome: false,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			outcome := applyOptionalErrorFilters(err, testData.optionalFilters...)
			require.Equal(t, testData.expectedOutcome, outcome)
		})
	}
}
