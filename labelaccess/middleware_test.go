// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/mimir/blob/main/pkg/querier/labelaccess/middleware_test.go
// Provenance-includes-license: AGPL-3.0-only
// Provenance-includes-copyright: Grafana Labs

package labelaccess

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestNewLabelAccessMiddleware_InjectsMatchersIntoContext(t *testing.T) {
	lps := LabelPolicySet{
		"tenant1": {{Selector: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
		}}},
	}

	var captured LabelPolicySet
	handler := NewLabelAccessMiddleware(log.NewNopLogger()).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		captured, err = ExtractLabelMatchersContext(r.Context())
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/query", nil)
	require.NoError(t, InjectLabelMatchersHTTP(req, lps))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, len(lps), len(captured))
}

func TestNewLabelAccessMiddleware_NoHeader(t *testing.T) {
	var called bool
	handler := NewLabelAccessMiddleware(log.NewNopLogger()).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/query", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, called)
}

func TestNewLabelAccessMiddleware_InvalidHeader(t *testing.T) {
	handler := NewLabelAccessMiddleware(log.NewNopLogger()).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called with invalid header")
	}))

	req := httptest.NewRequest("GET", "/query", nil)
	req.Header.Set(HTTPHeaderKey, "not-valid-no-colon")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}
