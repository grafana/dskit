package grpcutil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/gogo/googleapis/google/rpc"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	msgErr = "this is an error"
)

func TestErrorToStatus(t *testing.T) {
	testCases := map[string]struct {
		err            error
		expectedStatus *status.Status
	}{
		"no error cannot be cast to status.Status": {
			err: nil,
		},
		"a random error cannot be cast to status.Status": {
			err: errors.New(msgErr),
		},
		"a wrapped error of a random error cannot be cast to status.Status": {
			err: fmt.Errorf("wrapped: %w", errors.New(msgErr)),
		},
		"a gRPC error built by gogo/status can be cast to status.Status": {
			err:            status.Error(codes.Internal, msgErr),
			expectedStatus: status.New(codes.Internal, msgErr),
		},
		"a wrapped error of a gRPC error built by gogo/status can be cast to status.Status": {
			err:            fmt.Errorf("wrapped: %w", status.Error(codes.Internal, msgErr)),
			expectedStatus: status.New(codes.Internal, msgErr),
		},
		"a gRPC error built by grpc/status can be cast to status.Status": {
			err:            grpcstatus.Error(codes.Internal, msgErr),
			expectedStatus: status.New(codes.Internal, msgErr),
		},
		"a wrapped error of a gRPC error built by grpc/status can be cast to status.Status": {
			err:            fmt.Errorf("wrapped: %w", grpcstatus.Error(codes.Internal, msgErr)),
			expectedStatus: status.New(codes.Internal, msgErr),
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			stat, ok := ErrorToStatus(testData.err)
			if testData.expectedStatus == nil {
				require.False(t, ok)
				require.Nil(t, stat)
			} else {
				require.True(t, ok)
				require.NotNil(t, stat)
				require.Equal(t, testData.expectedStatus.Code(), stat.Code())
				require.Equal(t, testData.expectedStatus.Message(), stat.Message())
			}
		})
	}
}

func TestErrorToStatusCode(t *testing.T) {
	testCases := map[string]struct {
		err                error
		expectedStatusCode codes.Code
	}{
		"no error returns codes.OK": {
			err:                nil,
			expectedStatusCode: codes.OK,
		},
		"a non-gRPC error returns codes.Unknown": {
			err:                errors.New(msgErr),
			expectedStatusCode: codes.Unknown,
		},
		"a wrapped non-gRPC error returns codes.Unknown": {
			err:                fmt.Errorf("wrapped: %w", errors.New(msgErr)),
			expectedStatusCode: codes.Unknown,
		},
		"a gRPC error built by gogo/status returns its code": {
			err:                status.Error(codes.Internal, msgErr),
			expectedStatusCode: codes.Internal,
		},
		"a wrapped error of a gRPC error built by gogo/status returns the gRPC error's code": {
			err:                fmt.Errorf("wrapped: %w", status.Error(codes.Unavailable, msgErr)),
			expectedStatusCode: codes.Unavailable,
		},
		"a gRPC error built by grpc/status returns its code": {
			err:                grpcstatus.Error(codes.FailedPrecondition, msgErr),
			expectedStatusCode: codes.FailedPrecondition,
		},
		"a wrapped error of a gRPC error built by grpc/status returns the gRPC error's code": {
			err:                fmt.Errorf("wrapped: %w", grpcstatus.Error(codes.ResourceExhausted, msgErr)),
			expectedStatusCode: codes.ResourceExhausted,
		},
		"a gRPC error with a non-standard gRPC error code returns that code": {
			err:                status.ErrorProto(&rpc.Status{Code: http.StatusBadRequest, Message: msgErr}),
			expectedStatusCode: http.StatusBadRequest,
		},
		"a wrapped error of a gRPC error with a non-standard gRPC error code returns the gRPC error's code": {
			err:                fmt.Errorf("wrapped: %w", status.ErrorProto(&rpc.Status{Code: http.StatusServiceUnavailable, Message: msgErr})),
			expectedStatusCode: http.StatusServiceUnavailable,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			statusCode := ErrorToStatusCode(testData.err)
			require.Equal(t, testData.expectedStatusCode, statusCode)
		})
	}
}

func TestStatus(t *testing.T) {
	stat := Status(codes.FailedPrecondition, "bad data")
	require.Equal(t, codes.FailedPrecondition, stat.Code())
	require.Equal(t, "bad data", stat.Message())
	require.Len(t, stat.Details(), 0)

	originalDetails := []proto.Message{&ErrorDetails{Cause: WRONG_CLUSTER_VERIFICATION_LABEL}, &ErrorDetails{Cause: UNKNOWN_CAUSE}}
	stat = Status(codes.FailedPrecondition, "bad data", originalDetails...)
	require.Equal(t, codes.FailedPrecondition, stat.Code())
	require.Equal(t, "bad data", stat.Message())
	details := stat.Details()
	require.Len(t, details, 2)
	for i := range details {
		det, ok := details[i].(*ErrorDetails)
		require.True(t, ok)
		require.Equal(t, originalDetails[i], det)
	}

	badDetails := []proto.Message{nil}
	stat = Status(codes.Internal, "bad details", badDetails...)
	require.Equal(t, codes.InvalidArgument, stat.Code())
	require.Equal(t, `error while creating details for a Status with code Internal and error message "bad details": proto: Marshal called with nil`, stat.Message())
}

func TestIsCanceled(t *testing.T) {
	testCases := map[string]struct {
		err             error
		expectedOutcome bool
	}{
		"context.Canceled returns true": {
			err:             context.Canceled,
			expectedOutcome: true,
		},
		"a gRPC context.Canceled returns true": {
			err:             status.Error(codes.Canceled, context.Canceled.Error()),
			expectedOutcome: true,
		},
		"a wrapped gRPC context.Canceled returns true": {
			err:             fmt.Errorf("wrapped: %w", status.Error(codes.Canceled, context.Canceled.Error())),
			expectedOutcome: true,
		},
		"a random error returns false": {
			err:             errors.New(msgErr),
			expectedOutcome: false,
		},
		"a wrapped random error returns false": {
			err:             fmt.Errorf("wrapped: %w", errors.New(msgErr)),
			expectedOutcome: false,
		},
		"a gRPC error with code different from codes.Canceled returns false": {
			err:             status.Error(codes.Internal, msgErr),
			expectedOutcome: false,
		},
		"a wrapped gRPC error with code different from codes.Canceled returns false": {
			err:             fmt.Errorf("wrapped: %w", status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error())),
			expectedOutcome: false,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			isCanceled := IsCanceled(testData.err)
			require.Equal(t, testData.expectedOutcome, isCanceled)
		})
	}
}
