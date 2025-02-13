package httpgrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/dskit/clusterutil"
)

func TestAppendMessageSizeToOutgoingContext(t *testing.T) {
	ctx := context.Background()

	req := &HTTPRequest{
		Method: "GET",
		Url:    "/test",
	}

	ctx = AppendRequestMetadataToContext(ctx, req)

	md, exists := metadata.FromOutgoingContext(ctx)
	require.True(t, exists)

	require.Equal(t, []string{"GET"}, md.Get(MetadataMethod))
	require.Equal(t, []string{"/test"}, md.Get(MetadataURL))
}

func TestToHeader(t *testing.T) {
	grpcHeaders := []*Header{
		{Key: "X-Header", Values: []string{"a", "b", "c"}},
		{Key: "traceparent", Values: []string{"01234"}},
	}
	httpHeaders := http.Header{}
	ToHeader(grpcHeaders, httpHeaders)

	require.Equal(t, http.Header{
		"X-Header":    []string{"a", "b", "c"},
		"Traceparent": []string{"01234"},
	}, httpHeaders)
	require.Equal(t, "01234", httpHeaders.Get("traceparent"))
}

func TestErrorf(t *testing.T) {
	code := 400
	errMsg := "this is an error"
	expectedHTTPResponse := &HTTPResponse{
		Code: int32(code),
		Body: []byte(errMsg),
	}
	err := Error(code, errMsg)
	stat, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, code, int(stat.Code()))
	require.Equal(t, errMsg, stat.Message())
	checkDetailAsHTTPResponse(t, expectedHTTPResponse, stat)
}

func TestErrorFromHTTPResponse(t *testing.T) {
	var code int32 = 400
	errMsg := "this is an error"
	headers := []*Header{{Key: "X-Header", Values: []string{"a", "b", "c"}}}
	resp := &HTTPResponse{
		Code:    code,
		Headers: headers,
		Body:    []byte(errMsg),
	}
	err := ErrorFromHTTPResponse(resp)
	require.Error(t, err)
	stat, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, code, int32(stat.Code()))
	require.Equal(t, errMsg, stat.Message())
	checkDetailAsHTTPResponse(t, resp, stat)
}

func TestHTTPResponseFromError(t *testing.T) {
	msgErr := "this is an error"
	testCases := map[string]struct {
		err                  error
		isGRPCError          bool
		isHTTPGRCPError      bool
		expectedHTTPResponse *HTTPResponse
	}{
		"no error cannot be parsed to an HTTPResponse": {
			err: nil,
		},
		"a random error cannot be parsed to an HTTPResponse": {
			err: errors.New(msgErr),
		},
		"a gRPC error built by gogo/status cannot be parsed to an HTTPResponse": {
			err: status.Error(codes.Internal, msgErr),
		},
		"a gRPC error built by grpc/status cannot be parsed to an HTTPResponse": {
			err: grpcstatus.Error(codes.Internal, msgErr),
		},
		"a gRPC error built by httpgrpc can be parsed to an HTTPResponse": {
			err:                  Error(400, msgErr),
			expectedHTTPResponse: &HTTPResponse{Code: 400, Body: []byte(msgErr)},
		},
		"a wrapped gRPC error built by httpgrpc can be parsed to an HTTPResponse": {
			err:                  fmt.Errorf("wrapped: %w", Error(400, msgErr)),
			expectedHTTPResponse: &HTTPResponse{Code: 400, Body: []byte(msgErr)},
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			resp, ok := HTTPResponseFromError(testData.err)
			if testData.expectedHTTPResponse == nil {
				require.False(t, ok)
				require.Nil(t, resp)
			} else {
				require.True(t, ok)

			}
		})
	}
}

func TestFromHTTPRequestWithCluster(t *testing.T) {
	t.Run("with matching header", func(t *testing.T) {
		r, err := http.NewRequest(http.MethodGet, "http://server", io.NopCloser(bytes.NewBuffer(nil)))
		require.NoError(t, err)
		r.Header.Set(clusterutil.ClusterVerificationLabelHeader, "test")
		// TODO: Test with metric.
		gr, err := FromHTTPRequestWithCluster(r, "test", nil)
		require.NoError(t, err)
		require.Equal(t, []*Header{
			{
				Key:    clusterutil.ClusterVerificationLabelHeader,
				Values: []string{"test"},
			},
		}, gr.Headers)
	})
	t.Run("without header", func(t *testing.T) {
		r, err := http.NewRequest(http.MethodGet, "http://server", nil)
		require.NoError(t, err)
		// TODO: Test with metric.
		_, err = FromHTTPRequestWithCluster(r, "test", nil)
		require.EqualError(t, err, `httpgrpc.FromHTTPRequestWithCluster: "X-Cluster" header should be "test", but is ""`)
	})
	t.Run("with mismatched header", func(t *testing.T) {
		r, err := http.NewRequest(http.MethodGet, "http://server", nil)
		require.NoError(t, err)
		r.Header.Set(clusterutil.ClusterVerificationLabelHeader, "prod")
		// TODO: Test with metric.
		_, err = FromHTTPRequestWithCluster(r, "test", nil)
		require.EqualError(t, err, `httpgrpc.FromHTTPRequestWithCluster: "X-Cluster" header should be "test", but is "prod"`)
	})
}

func checkDetailAsHTTPResponse(t *testing.T, httpResponse *HTTPResponse, stat *status.Status) {
	details := stat.Details()
	require.Len(t, details, 1)
	respDetails, ok := details[0].(*HTTPResponse)
	require.True(t, ok)
	require.NotNil(t, respDetails)
	require.Equal(t, httpResponse.Code, respDetails.Code)
	require.Equal(t, httpResponse.Headers, respDetails.Headers)
	require.Equal(t, httpResponse.Body, respDetails.Body)
}
