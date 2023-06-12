package grpcclient

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/grafana/dskit/crypto/tls"
)

type fakeDialOption struct {
	grpc.EmptyDialOption

	callOpts                []grpc.CallOption
	isInsecure              bool
	unaryClientInterceptor  grpc.UnaryClientInterceptor
	streamClientInterceptor grpc.StreamClientInterceptor
	keepaliveParams         keepalive.ClientParameters
}

func (o fakeDialOption) Equal(other fakeDialOption) bool {
	if len(o.callOpts) != len(other.callOpts) {
		return false
	}

	for i, arg := range o.callOpts {
		if maxRecv, ok := arg.(grpc.MaxRecvMsgSizeCallOption); ok {
			if maxRecv != other.callOpts[i].(grpc.MaxRecvMsgSizeCallOption) {
				return false
			}
			continue
		}
		if maxSend, ok := arg.(grpc.MaxSendMsgSizeCallOption); ok {
			if maxSend != other.callOpts[i].(grpc.MaxSendMsgSizeCallOption) {
				return false
			}
			continue
		}
	}

	hasUnaryInterceptor := o.unaryClientInterceptor != nil
	otherHasUnaryInterceptor := other.unaryClientInterceptor != nil
	hasStreamInterceptor := o.streamClientInterceptor != nil
	otherHasStreamInterceptor := other.streamClientInterceptor != nil

	return o.isInsecure == other.isInsecure && hasUnaryInterceptor == otherHasUnaryInterceptor &&
		hasStreamInterceptor == otherHasStreamInterceptor && o.keepaliveParams == other.keepaliveParams
}

func TestConfig(t *testing.T) {
	origWithDefaultCallOptions := withDefaultCallOptions
	origWithUnaryInterceptor := withUnaryInterceptor
	origWithStreamInterceptor := withStreamInterceptor
	origWithKeepaliveParams := withKeepaliveParams
	origWithInsecure := tls.WithInsecure
	t.Cleanup(func() {
		withDefaultCallOptions = origWithDefaultCallOptions
		withUnaryInterceptor = origWithUnaryInterceptor
		withStreamInterceptor = origWithStreamInterceptor
		withKeepaliveParams = origWithKeepaliveParams
		tls.WithInsecure = origWithInsecure
	})

	withDefaultCallOptions = func(cos ...grpc.CallOption) grpc.DialOption {
		t.Log("Received call options", "options", cos)
		return fakeDialOption{callOpts: cos}
	}
	withUnaryInterceptor = func(f grpc.UnaryClientInterceptor) grpc.DialOption {
		t.Log("Received unary client interceptor", f)
		return fakeDialOption{unaryClientInterceptor: f}
	}
	withStreamInterceptor = func(f grpc.StreamClientInterceptor) grpc.DialOption {
		t.Log("Received stream client interceptor", f)
		return fakeDialOption{streamClientInterceptor: f}
	}
	withKeepaliveParams = func(kp keepalive.ClientParameters) grpc.DialOption {
		t.Log("Received keepalive params", kp)
		return fakeDialOption{
			keepaliveParams: kp,
		}
	}
	tls.WithInsecure = func() grpc.DialOption {
		return fakeDialOption{isInsecure: true}
	}

	cfg := Config{}
	const keepaliveTime = 10
	const keepaliveTimeout = 20
	expOpts := []grpc.DialOption{
		fakeDialOption{isInsecure: true},
		fakeDialOption{callOpts: []grpc.CallOption{
			grpc.MaxCallRecvMsgSize(0),
			grpc.MaxCallSendMsgSize(0),
		}},
		fakeDialOption{
			unaryClientInterceptor: middleware.ChainUnaryClient(),
		},
		fakeDialOption{
			streamClientInterceptor: middleware.ChainStreamClient(),
		},
		fakeDialOption{
			keepaliveParams: keepalive.ClientParameters{
				Time:                keepaliveTime * time.Second,
				Timeout:             keepaliveTimeout * time.Second,
				PermitWithoutStream: true,
			},
		},
	}

	opts, err := cfg.DialOption(nil, nil, keepaliveTime, keepaliveTimeout)
	require.NoError(t, err)

	assert.Empty(t, cmp.Diff(expOpts, opts))
}
