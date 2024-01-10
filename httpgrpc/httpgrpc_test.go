package httpgrpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
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

func TestErrorf(t *testing.T) {
	code := 400
	errMsg := "this is an error"
	expectedHTTPResponse := &HTTPResponse{
		Code: int32(code),
		Body: []byte(errMsg),
	}
	err := Errorf(code, errMsg)
	stat, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, code, int(stat.Code()))
	require.Equal(t, errMsg, stat.Message())
	checkDetailAsHTTPResponse(t, expectedHTTPResponse, stat)
}

func TestHTTPErrorf(t *testing.T) {
	httpErrorStatusCode := 400
	httpErrorStatusMsg := "this is an error"
	expectedHTTPResponse := &HTTPResponse{
		Code: int32(httpErrorStatusCode),
		Body: []byte(httpErrorStatusMsg),
	}

	err := HTTPErrorf(httpErrorStatusCode, httpErrorStatusMsg)
	require.Error(t, err)
	require.Equal(t, expectedHTTPResponse, err.GetHTTPResponse())

	stat, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, stat.Code())
	require.Equal(t, httpErrorStatusMsg, stat.Message())
	checkDetailAsHTTPResponse(t, expectedHTTPResponse, stat)
}

func TestErrorFromHTTPResponse(t *testing.T) {
	const (
		code   int32 = 400
		errMsg       = "this is an error"
	)
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

func TestHTTPErrorFromHTTPResponse(t *testing.T) {
	const (
		code   = 400
		errMsg = "this is an error"
	)
	headers := []*Header{{Key: "X-Header", Values: []string{"a", "b", "c"}}}
	resp := &HTTPResponse{
		Code:    code,
		Headers: headers,
		Body:    []byte(errMsg),
	}
	err := HTTPErrorFromHTTPResponse(resp)
	require.Error(t, err)
	stat, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, stat.Code())
	require.Equal(t, errMsg, stat.Message())
	checkDetailAsHTTPResponse(t, resp, stat)
}

func TestHTTPResponseFromError(t *testing.T) {
	const msgErr = "this is an error"
	var resp = &HTTPResponse{Code: 400, Body: []byte(msgErr)}
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
			err: fmt.Errorf(msgErr),
		},
		"a gRPC error built by gogo/status cannot be parsed to an HTTPResponse": {
			err: status.Error(codes.Internal, msgErr),
		},
		"a gRPC error built by grpc/status cannot be parsed to an HTTPResponse": {
			err: grpcstatus.Error(codes.Internal, msgErr),
		},
		"a gRPC error built by httpgrpc can be parsed to an HTTPResponse": {
			err:                  Errorf(400, msgErr),
			expectedHTTPResponse: resp,
		},
		"a wrapped gRPC error built by httpgrpc can be parsed to an HTTPResponse": {
			err:                  fmt.Errorf("wrapped: %w", Errorf(400, msgErr)),
			expectedHTTPResponse: resp,
		},
		"an instance of HTTPError can be parsed to an HTTPResponse": {
			err:                  HTTPErrorFromHTTPResponse(resp),
			expectedHTTPResponse: resp,
		},
		"a wrapped instance of HTTPError can be parsed to an HTTPResponse": {
			err:                  fmt.Errorf("wrapped: %w", HTTPErrorFromHTTPResponse(resp)),
			expectedHTTPResponse: resp,
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
				checkEqualHTTPResponses(t, testData.expectedHTTPResponse, resp)
			}
		})
	}
}

func checkDetailAsHTTPResponse(t *testing.T, httpResponse *HTTPResponse, stat *status.Status) {
	details := stat.Details()
	require.Len(t, details, 1)
	respDetails, ok := details[0].(*HTTPResponse)
	require.True(t, ok)
	require.NotNil(t, respDetails)
	checkEqualHTTPResponses(t, httpResponse, respDetails)
}

func checkEqualHTTPResponses(t *testing.T, expectedResp, resp *HTTPResponse) {
	require.Equal(t, expectedResp.GetCode(), resp.GetCode())
	require.Equal(t, expectedResp.GetHeaders(), resp.GetHeaders())
	require.Equal(t, expectedResp.GetBody(), resp.GetBody())
}
