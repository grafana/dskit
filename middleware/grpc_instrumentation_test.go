// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/grpc_instrumentation_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/grafana/dskit/httpgrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorCode_NoError(t *testing.T) {
	a := errorCode(nil)
	assert.Equal(t, "2xx", a)
}

func TestErrorCode_Any5xx(t *testing.T) {
	err := httpgrpc.Errorf(http.StatusNotImplemented, "Fail")
	a := errorCode(err)
	assert.Equal(t, "5xx", a)
}

func TestErrorCode_Any4xx(t *testing.T) {
	err := httpgrpc.Errorf(http.StatusConflict, "Fail")
	a := errorCode(err)
	assert.Equal(t, "4xx", a)
}

func TestErrorCode_Canceled(t *testing.T) {
	err := status.Errorf(codes.Canceled, "Fail")
	a := errorCode(err)
	assert.Equal(t, "cancel", a)
}

func TestErrorCode_Unknown(t *testing.T) {
	err := status.Errorf(codes.Unknown, "Fail")
	a := errorCode(err)
	assert.Equal(t, "error", a)
}
