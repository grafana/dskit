package middleware

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGRPCResponse(t *testing.T) {
	err := &GRPCError{
		Message:   "this is a loggable error",
		ShouldLog: true,
	}

	resp := GRPCResponse{
		Error: err,
	}
	anotherResp := FakeGRPCResponse{
		GRPCResponse: resp,
		Status:       "INVALID_REQUEST",
	}

	fetcher := func() interface{} {
		return &anotherResp
	}

	res := fetcher()

	resResp, ok := res.(LoggableError)
	require.True(t, ok)
	require.Equal(t, err.Message, resResp.GetError().GetMessage())
}
