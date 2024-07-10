package grpcclient_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/dskit/grpcclient"
)

func TestRateLimiterFailureResultsInResourceExhaustedError(t *testing.T) {
	config := grpcclient.Config{
		RateLimitBurst: 0,
		RateLimit:      0,
	}
	conn := grpc.ClientConn{}
	invoker := func(context.Context, string, interface{}, interface{}, *grpc.ClientConn, ...grpc.CallOption) error {
		return nil
	}

	limiter := grpcclient.NewRateLimiter(&config)
	err := limiter(context.Background(), "methodName", "", "expectedReply", &conn, invoker)

	if se, ok := err.(interface {
		GRPCStatus() *status.Status
	}); ok {
		assert.Equal(t, se.GRPCStatus().Code(), codes.ResourceExhausted)
		assert.Equal(t, se.GRPCStatus().Message(), "rate: Wait(n=1) exceeds limiter's burst 0")
	} else {
		assert.Fail(t, "Could not convert error into expected Status type")
	}
}
