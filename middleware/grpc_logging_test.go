// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/grpc_logging_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

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

type doNotLogError struct{ Err error }

func (i doNotLogError) Error() string                                     { return i.Err.Error() }
func (i doNotLogError) Unwrap() error                                     { return i.Err }
func (i doNotLogError) ShouldLog(_ context.Context, _ time.Duration) bool { return false }

func TestGrpcLogging(t *testing.T) {
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "Test"}
	for _, tc := range []struct {
		err         error
		logContains []string
	}{{
		err:         context.Canceled,
		logContains: []string{"level=debug", "context canceled"},
	}, {
		err:         errors.New("yolo"),
		logContains: []string{"level=warn", "err=yolo"},
	}, {
		err:         nil,
		logContains: []string{"level=debug", "method=Test"},
	}, {
		err:         doNotLogError{Err: errors.New("yolo")},
		logContains: nil,
	}} {
		t.Run("", func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			logger := log.NewLogfmtLogger(buf)
			l := GRPCServerLog{Log: logger, WithRequest: true, DisableRequestSuccessLog: false}

			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				return nil, tc.err
			}

			_, err := l.UnaryServerInterceptor(ctx, nil, info, handler)
			require.ErrorIs(t, tc.err, err)

			if len(tc.logContains) == 0 {
				require.Empty(t, buf)
			}
			for _, content := range tc.logContains {
				require.Contains(t, buf.String(), content)
			}
		})
	}
}
