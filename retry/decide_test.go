package retry

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("status %d", e.code) }

func isRetryableStatus(err error) bool {
	var status *statusError
	return errors.As(err, &status) && status.code >= 500
}

func TestDecider_composesWithAnIf(t *testing.T) {
	decide := func(err error) Decision {
		if d := Marked(err); d.Retryable {
			return d
		}
		return Decision{Retryable: isRetryableStatus(err)}
	}
	var _ Decider = decide

	assert.Equal(t, Decision{Retryable: true}, decide(MarkRetryable(&statusError{code: 404})),
		"a mark wins over the fallback")
	assert.Equal(t, Decision{Retryable: true}, decide(&statusError{code: 503}))
	assert.Equal(t, Decision{}, decide(&statusError{code: 404}))
	assert.Equal(t, Decision{}, decide(errors.New("boom")))
}
