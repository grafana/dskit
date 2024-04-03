// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/grpc_logging_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func BenchmarkGRPCServerLog_UnaryServerInterceptor_NoError(b *testing.B) {
	logger := level.NewFilter(log.NewNopLogger(), level.AllowError())
	l := GRPCServerLog{Log: logger, WithRequest: false, DisableRequestSuccessLog: true}
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "Test"}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, _ = l.UnaryServerInterceptor(ctx, nil, info, handler)
	}
}

func TestGrpcLogging(t *testing.T) {
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "Test"}
	for _, tc := range []struct {
		inputErr    error
		expectedErr error
		logContains []string
	}{{
		inputErr:    context.Canceled,
		expectedErr: context.Canceled,
		logContains: []string{"level=debug", "context canceled"},
	}, {
		inputErr:    errors.New("yolo"),
		expectedErr: errors.New("yolo"),
		logContains: []string{"level=warn", "err=yolo"},
	}, {
		inputErr:    nil,
		expectedErr: nil,
		logContains: []string{"level=debug", "method=Test"},
	}, {
		inputErr:    DoNotLogError{Err: errors.New("yolo")},
		expectedErr: DoNotLogError{Err: errors.New("yolo")},
		logContains: nil,
	}, {
		inputErr:    sampledError{err: errors.New("yolo"), shouldLog: true, reason: "sampled 1/10"},
		expectedErr: fmt.Errorf("%w (sampled 1/10)", sampledError{err: errors.New("yolo"), shouldLog: true, reason: "sampled 1/10"}),
		logContains: []string{`err="yolo (sampled 1/10)"`},
	}, {
		inputErr: sampledError{err: errors.New("yolo"), shouldLog: false, reason: "sampled 1/10"},

		// The returned error should have the "sampled" suffix because it has been effectively sampled even if not logged.
		expectedErr: fmt.Errorf("%w (sampled 1/10)", sampledError{err: errors.New("yolo"), shouldLog: false, reason: "sampled 1/10"}),
		logContains: nil,
	}} {
		t.Run("", func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			logger := log.NewLogfmtLogger(buf)
			l := GRPCServerLog{Log: logger, WithRequest: true, DisableRequestSuccessLog: false}

			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				return nil, tc.inputErr
			}

			_, err := l.UnaryServerInterceptor(ctx, nil, info, handler)

			if tc.expectedErr != nil {
				require.EqualError(t, err, tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
			}

			// The input error should be preserved in the chain.
			require.ErrorIs(t, err, tc.inputErr)

			if len(tc.logContains) == 0 {
				require.Empty(t, buf)
			}
			for _, content := range tc.logContains {
				require.Contains(t, buf.String(), content)
			}
		})
	}
}

type sampledError struct {
	err       error
	shouldLog bool
	reason    string
}

func (e sampledError) Error() string                              { return e.err.Error() }
func (e sampledError) Unwrap() error                              { return e.err }
func (e sampledError) ShouldLog(_ context.Context) (bool, string) { return e.shouldLog, e.reason }
