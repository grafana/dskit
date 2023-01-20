package multierror

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestMultiError_Is(t *testing.T) {
	testCases := map[string]struct {
		sourceErrors []error
		target       error
		is           bool
	}{
		"adding a context cancellation doesn't lose the information": {
			sourceErrors: []error{context.Canceled},
			target:       context.Canceled,
			is:           true,
		},
		"adding multiple context cancellations doesn't lose the information": {
			sourceErrors: []error{context.Canceled, context.Canceled},
			target:       context.Canceled,
			is:           true,
		},
		"adding wrapped context cancellations doesn't lose the information": {
			sourceErrors: []error{errors.New("some error"), errors.Wrap(context.Canceled, "some message")},
			target:       context.Canceled,
			is:           true,
		},
		"adding a nil error doesn't lose the information": {
			sourceErrors: []error{errors.New("some error"), errors.Wrap(context.Canceled, "some message"), nil},
			target:       context.Canceled,
			is:           true,
		},
		"no errors are not a context canceled error": {
			sourceErrors: nil,
			target:       context.Canceled,
			is:           false,
		},
		"no errors are a nil error": {
			sourceErrors: nil,
			target:       nil,
			is:           true,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			mErr := MultiError{}
			for _, err := range testCase.sourceErrors {
				mErr.Add(err)
			}
			assert.Equal(t, testCase.is, errors.Is(mErr.Err(), testCase.target))
		})
	}
}
