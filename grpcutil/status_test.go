package grpcutil

import (
	"fmt"
	"testing"

	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestErrorToStatus(t *testing.T) {
	msgErr := "this is an error"
	testCases := map[string]struct {
		err            error
		expectedStatus *status.Status
	}{
		"no error cannot be cast to status.Status": {
			err: nil,
		},
		"a random error cannot be cast to status.Status": {
			err: fmt.Errorf(msgErr),
		},
		"a wrapped error of a random error cannot be cast to status.Status": {
			err: fmt.Errorf("wrapped: %w", fmt.Errorf(msgErr)),
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
