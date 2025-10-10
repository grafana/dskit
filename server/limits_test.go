package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	protobuf "github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/dskit/test"
)

func TestGrpcLimitCheckMalformedMethodName(t *testing.T) {
	const badMethodName = "bad_method_name"

	ts := &testServer{finishRequest: make(chan struct{})}
	ml := &methodLimiter{protectedMethod: badMethodName}

	limitCheck := newGrpcInflightLimitCheck(ml, log.NewNopLogger())

	c := setupGrpcServerWithCheckAndClient(t, ts, limitCheck)

	out := &protobuf.Empty{}
	err := c.(*fakeServerClient).cc.Invoke(context.Background(), badMethodName, &protobuf.Empty{}, out)

	require.Error(t, err)
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unimplemented, s.Code())
	require.Contains(t, s.Message(), "malformed method name")
	require.Equal(t, int64(0), ml.allInflight.Load())
	require.Equal(t, int64(0), ml.protectedMethodInflight.Load())
}

func checkGrpcStatusError(t *testing.T, err error, code codes.Code, msg string) {
	require.Error(t, err)
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, code, s.Code())
	require.Equal(t, msg, s.Message())
}

func callToSucceed(ctx context.Context, c FakeServerClient) error {
	_, err := c.Succeed(ctx, &protobuf.Empty{})
	return err
}

func callToSleep(ctx context.Context, c FakeServerClient) error {
	_, err := c.Sleep(ctx, &protobuf.Empty{})
	return err
}

func callToStreaming(msgsPerStreamCall int) func(ctx context.Context, c FakeServerClient) error {
	return func(ctx context.Context, c FakeServerClient) error {
		rcvd := 0
		s, err := c.StreamSleep(ctx, &protobuf.Empty{})
		if err != nil {
			return err
		}

		for {
			_, err := s.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			rcvd++
		}

		if rcvd != msgsPerStreamCall {
			return fmt.Errorf("invalid number of received messages: %d", rcvd)
		}
		return nil
	}
}

func TestGrpcLimitCheckUnary(t *testing.T) {
	const msgsPerStreamCall = 5
	const protectedMethodName = "/server.FakeServer/Succeed"

	t.Run("grpcLimit=5, methodLimit=3", func(t *testing.T) {
		testGrpcLimitCheckWithMethodLimiter(t, 5, 3, msgsPerStreamCall, protectedMethodName, callToSucceed, callToSleep, callToStreaming(msgsPerStreamCall))
	})

	t.Run("grpcLimit=unlimited, methodLimit=3", func(t *testing.T) {
		testGrpcLimitCheckWithMethodLimiter(t, 5, 3, msgsPerStreamCall, protectedMethodName, callToSucceed, callToSleep, callToStreaming(msgsPerStreamCall))
	})

	t.Run("grpcLimit=5, methodLimit=unlimited", func(t *testing.T) {
		testGrpcLimitCheckWithMethodLimiter(t, 5, 0, msgsPerStreamCall, protectedMethodName, callToSucceed, callToSleep, callToStreaming(msgsPerStreamCall))
	})
}

func TestGrpcLimitCheckStreaming(t *testing.T) {
	const msgsPerStreamCall = 5
	const protectedMethodName = "/server.FakeServer/StreamSleep"

	t.Run("grpcLimit=5, methodLimit=3", func(t *testing.T) {
		testGrpcLimitCheckWithMethodLimiter(t, 5, 3, msgsPerStreamCall, protectedMethodName, callToStreaming(msgsPerStreamCall), callToSucceed, callToSleep)
	})

	t.Run("grpcLimit=unlimited, methodLimit=3", func(t *testing.T) {
		testGrpcLimitCheckWithMethodLimiter(t, 5, 3, msgsPerStreamCall, protectedMethodName, callToStreaming(msgsPerStreamCall), callToSucceed, callToSleep)
	})

	t.Run("grpcLimit=5, methodLimit=unlimited", func(t *testing.T) {
		testGrpcLimitCheckWithMethodLimiter(t, 5, 0, msgsPerStreamCall, protectedMethodName, callToStreaming(msgsPerStreamCall), callToSucceed, callToSleep)
	})
}

func testGrpcLimitCheckWithMethodLimiter(
	t *testing.T,
	inflightLimit, protectedMethodLimit, msgsPerStreamCall int,
	protectedMethodName string,
	callToProtectedMethod func(ctx context.Context, c FakeServerClient) error,
	callsToUnprotectedMethods ...func(ctx context.Context, c FakeServerClient) error,
) {
	if inflightLimit != 0 && inflightLimit < protectedMethodLimit {
		t.Fatal("invalid combination of parameters for this test")
	}

	ts := &testServer{finishRequest: make(chan struct{}), msgPerStreamCall: msgsPerStreamCall}
	ml := &methodLimiter{protectedMethod: protectedMethodName, allInflightLimit: inflightLimit, protectedMethodInflightLimit: protectedMethodLimit}

	limitCheck := newGrpcInflightLimitCheck(ml, log.NewNopLogger())

	c := setupGrpcServerWithCheckAndClient(t, ts, limitCheck)

	// start background requests for method with method limit (Succeed)
	started := sync.WaitGroup{}
	finished := sync.WaitGroup{}

	if protectedMethodLimit > 0 {
		for i := 0; i < protectedMethodLimit; i++ {
			started.Add(1)
			finished.Add(1)

			go func() {
				started.Done()
				defer finished.Done()

				err := callToProtectedMethod(context.Background(), c)
				require.NoError(t, err)
			}()
		}

		// Wait until all goroutines start and all calls are in-flight calls for protected method.
		started.Wait()
		test.Poll(t, 1*time.Second, int64(protectedMethodLimit), func() interface{} {
			return ml.protectedMethodInflight.Load()
		})

		// Another request to limited method should fail.
		err := callToProtectedMethod(context.Background(), c)
		checkGrpcStatusError(t, err, codes.ResourceExhausted, "too many requests to "+protectedMethodName)
	}

	for _, fn := range append(callsToUnprotectedMethods, callToProtectedMethod) {
		// Requests to all method with abort flag should always fail (that's how our check works)
		err := fn(metadata.AppendToOutgoingContext(context.Background(), metaAbortRequest, "true"), c)
		checkGrpcStatusError(t, err, codes.Aborted, "aborted")
	}

	// However we can start more requests to different (unprotected) method
	extraCalls := 10
	if inflightLimit > 0 {
		extraCalls = inflightLimit - protectedMethodLimit
	}

	for i := 0; i < extraCalls; i++ {
		started.Add(1)
		finished.Add(1)

		go func() {
			started.Done()
			defer finished.Done()

			for _, fn := range callsToUnprotectedMethods {
				err := fn(context.Background(), c)
				require.NoError(t, err)
			}
		}()
	}

	// Wait until all goroutines start and all calls are in-flight.
	started.Wait()
	test.Poll(t, 1*time.Second, int64(protectedMethodLimit+extraCalls), func() interface{} {
		return ml.allInflight.Load()
	})

	if inflightLimit > 0 {
		// But now we're really at the limit -- we used all grpc inflight limit calls. Everything should return codes.Unavailable now.
		for _, fn := range append(callsToUnprotectedMethods, callToProtectedMethod) {
			err := fn(context.Background(), c)
			checkGrpcStatusError(t, err, codes.Unavailable, "too many requests")

			// Requests with abort flag are aborted early, without doing being blocked.
			err = fn(metadata.AppendToOutgoingContext(context.Background(), metaAbortRequest, "true"), c)
			checkGrpcStatusError(t, err, codes.Aborted, "aborted")
		}
	}

	// unblock all pending and future requests, and wait for goroutines to finish
	close(ts.finishRequest)
	finished.Wait()

	require.Equal(t, int64(0), ml.allInflight.Load())
	require.Equal(t, int64(0), ml.protectedMethodInflight.Load())

	// Another request to protected or unprotected method should succeed again.
	for _, fn := range append(callsToUnprotectedMethods, callToProtectedMethod) {
		err := fn(context.Background(), c)
		require.NoError(t, err)

		// Unless we pass abort-request header.
		err = fn(metadata.AppendToOutgoingContext(context.Background(), metaAbortRequest, "true"), c)
		checkGrpcStatusError(t, err, codes.Aborted, "aborted")
	}
}

// This test covers an edge case described in https://github.com/grafana/dskit/issues/749
// A shortcut to an early return right after checking the tap handle introduced https://github.com/grpc/grpc-go/pull/8439
// may cause stats handler not be executed, and thus the RPCCallFinished not being called.
// In this test we simulate that by waiting in the tap handle until the request context is already expired.
// Then we check that actually the RPCCallFinished was not called.
// Then we execute the scheduled timer function, and check that RPCCallFinished is called from there.
func TestGrpcLimitCheckWithContextDeadlineExpiredRightAfterTapHandleCheck(t *testing.T) {
	rpcCallFinishedCalled := atomic.NewBool(false)

	ts := &testServer{finishRequest: make(chan struct{}), msgPerStreamCall: 5}
	// All requests finish instantly.
	close(ts.finishRequest)
	ml := funcMethodLimiter{
		rpcCallStarting: func(ctx context.Context, methodName string, md metadata.MD) (context.Context, error) {
			// When this is called from the tap handle, the request shouldn't have expired yet.
			select {
			case <-ctx.Done():
				t.Error("context should not be done yet")
			default:
			}

			// This tap handle waits until the request context is already expired.
			<-ctx.Done()
			return ctx, nil
		},
		rpcCallProcessing: func(_ context.Context, _ string) (func(error), error) { return nil, nil },
		rpcCallFinished: func(ctx context.Context) {
			rpcCallFinishedCalled.Store(true)
		},
	}

	timerFuncs := make(chan func(), 1)

	limitCheck := newGrpcInflightLimitCheck(ml, log.NewNopLogger())
	limitCheck.timeAfterFuncMock = func(d time.Duration, f func()) testableTimer {
		assert.Equal(t, unprocessedRequestCheckTimeout, d)
		timerFuncs <- f
		return testableTimer{stop: func() bool {
			t.Error("Timer.Stop() should not be called, because the request context was already expired when TapHandle was called. If stop was called, maybe the gRPC shortcut was removed?")
			return false
		}}
	}

	c := setupGrpcServerWithCheckAndClient(t, ts, limitCheck)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := c.Succeed(ctx, &protobuf.Empty{})
	require.Error(t, err)
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.DeadlineExceeded, s.Code())

	// Get the function that was scheduled with a timer.
	var timerFunc func()
	select {
	case timerFunc = <-timerFuncs:
	case <-time.After(time.Second):
		t.Fatal("expected timer function to be scheduled when the request is already finished")
	}

	require.False(t, rpcCallFinishedCalled.Load(), "RPCFinishedCall should not have been called yet, because of the shortcut in the grpc library."+
		" If this test fails here, then maybe grpc library changed, please review and check whether the test has to be updated or all this logic should be removed.")

	// We know that Timer.Stop() wasn't called (because it would be stuck on trying to push to stopResult channel).
	// Execute the timer.
	timerFunc()

	// Now check that RPCCallFinished was called.
	require.True(t, rpcCallFinishedCalled.Load(), "expected RPCCallFinished to be called when the unprocessed request timer fires")
}

// This test covers an edge case of an edge case described in https://github.com/grafana/dskit/issues/749
// It describes a scenario where the timer fires but the request context is still not canceled.
// In this scenario the request is processed and context is not canceled.
func TestGrpcLimitCheckTimerFiresButRequestIsBeingProcessedCallsRPCCallFinishedOnlyOnce(t *testing.T) {
	rpcCallFinishedCalled := atomic.NewInt64(0)

	ts := &testServer{finishRequest: make(chan struct{}), msgPerStreamCall: 5}
	ml := funcMethodLimiter{
		rpcCallStarting: func(ctx context.Context, methodName string, md metadata.MD) (context.Context, error) {
			// When this is called from the tap handle, the request shouldn't have expired yet.
			select {
			case <-ctx.Done():
				t.Error("context should not be done yet")
			default:
			}

			// This tap handle waits until the request context is already expired.
			<-ctx.Done()
			return ctx, nil
		},
		rpcCallProcessing: func(_ context.Context, _ string) (func(error), error) { return nil, nil },
		rpcCallFinished: func(ctx context.Context) {
			rpcCallFinishedCalled.Inc()
		},
	}

	timerExecuted := make(chan struct{})
	limitCheck := newGrpcInflightLimitCheck(ml, log.NewNopLogger())
	limitCheck.timeAfterFuncMock = func(d time.Duration, f func()) testableTimer {
		// Fire immediately.
		go func() {
			f()
			close(timerExecuted)
		}()
		return testableTimer{stop: func() bool {
			// Tell we already fired the timer.
			return true
		}}
	}

	c := setupGrpcServerWithCheckAndClient(t, ts, limitCheck)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		// Let the request finish only after the timer is finished.
		// We're trying to force both the timer and the request to run RPCCallFinished.
		// (That should not happen).
		<-timerExecuted
		close(ts.finishRequest)
	}()

	_, err := c.Succeed(ctx, &protobuf.Empty{})
	require.Error(t, err)
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.DeadlineExceeded, s.Code())

	// Wait until timer has been executed.
	select {
	case <-timerExecuted:
	case <-time.After(time.Second):
		t.Fatal("expected timer function to be executed")
	}

	// Check that RPCCallFinished was called only once.
	require.Equal(t, int64(1), rpcCallFinishedCalled.Load(), "expected RPCCallFinished to be called only once")
}

// This checks that the bugfix timer for https://github.com/grafana/dskit/issues/749 is actually canceled before being executed.
func TestGrpcLimitCheckSuccessfulRequestCancelsTheTimer(t *testing.T) {
	rpcCallFinishedCalled := atomic.NewBool(false)

	ts := &testServer{finishRequest: make(chan struct{}), msgPerStreamCall: 5}
	// All requests finish instantly.
	close(ts.finishRequest)
	ml := funcMethodLimiter{
		rpcCallStarting: func(ctx context.Context, methodName string, md metadata.MD) (context.Context, error) {
			return ctx, nil
		},
		rpcCallProcessing: func(_ context.Context, _ string) (func(error), error) { return nil, nil },
		rpcCallFinished: func(ctx context.Context) {
			rpcCallFinishedCalled.Store(true)
		},
	}

	stopWasCalled := atomic.NewBool(false)

	limitCheck := newGrpcInflightLimitCheck(ml, log.NewNopLogger())
	limitCheck.timeAfterFuncMock = func(d time.Duration, f func()) testableTimer {
		assert.Equal(t, unprocessedRequestCheckTimeout, d)
		return testableTimer{stop: func() bool {
			stopWasCalled.Store(true)
			return true
		}}
	}

	c := setupGrpcServerWithCheckAndClient(t, ts, limitCheck)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := c.Succeed(ctx, &protobuf.Empty{})
	require.NoError(t, err)

	// Check that stop was called
	require.True(t, stopWasCalled.Load(), "expected Timer.Stop() to be called, because the request was successful")
	// Check that RPCCallFinished was called from the normal flow, not from the timer function.
	require.True(t, rpcCallFinishedCalled.Load(), "expected RPCCallFinished to be called when the request finished")
}

func setupGrpcServerWithCheckAndClient(t *testing.T, ts *testServer, g *grpcInflightLimitCheck) FakeServerClient {
	server := grpc.NewServer(grpc.InTapHandle(g.TapHandle), grpc.StatsHandler(g))
	RegisterFakeServerServer(server, ts)

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
	})

	cc, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = cc.Close()
	})

	return NewFakeServerClient(cc)
}

type testServer struct {
	FakeServer

	msgPerStreamCall int
	finishRequest    chan struct{}
}

func (ts *testServer) Succeed(_ context.Context, _ *protobuf.Empty) (*protobuf.Empty, error) {
	<-ts.finishRequest
	return &protobuf.Empty{}, nil
}

func (ts *testServer) Sleep(_ context.Context, _ *protobuf.Empty) (*protobuf.Empty, error) {
	<-ts.finishRequest
	return &protobuf.Empty{}, nil
}

func (ts *testServer) StreamSleep(_ *protobuf.Empty, stream FakeServer_StreamSleepServer) error {
	for i := 0; i < ts.msgPerStreamCall; i++ {
		_ = stream.Send(&protobuf.Empty{})
		<-ts.finishRequest
	}
	return nil
}

type methodLimiter struct {
	allInflightLimit int
	allInflight      atomic.Int64

	protectedMethod              string
	protectedMethodInflightLimit int
	protectedMethodInflight      atomic.Int64
}

type ctxKey string

const (
	ctxMethodName ctxKey = "method"

	metaAbortRequest = "abort-request"
)

func (m *methodLimiter) RPCCallStarting(ctx context.Context, methodName string, md metadata.MD) (context.Context, error) {
	if v := md.Get(metaAbortRequest); len(v) == 1 && v[0] == "true" {
		return ctx, status.Error(codes.Aborted, "aborted")
	}

	v := m.allInflight.Inc()
	if m.allInflightLimit > 0 && v > int64(m.allInflightLimit) {
		m.allInflight.Dec()
		return ctx, status.Error(codes.Unavailable, "too many requests")
	}

	if methodName == m.protectedMethod && m.protectedMethodInflightLimit > 0 {
		v := m.protectedMethodInflight.Inc()
		if v > int64(m.protectedMethodInflightLimit) {
			m.protectedMethodInflight.Dec()
			m.allInflight.Dec()
			return ctx, status.Error(codes.ResourceExhausted, "too many requests to "+m.protectedMethod)
		}
	}

	return context.WithValue(ctx, ctxMethodName, methodName), nil
}

func (m *methodLimiter) RPCCallProcessing(_ context.Context, _ string) (func(error), error) {
	return nil, nil
}

func (m *methodLimiter) RPCCallFinished(ctx context.Context) {
	m.allInflight.Dec()

	methodName := ctx.Value(ctxMethodName).(string)
	if methodName == m.protectedMethod && m.protectedMethodInflightLimit > 0 {
		m.protectedMethodInflight.Dec()
	}
}

type funcMethodLimiter struct {
	rpcCallStarting   func(ctx context.Context, methodName string, md metadata.MD) (context.Context, error)
	rpcCallProcessing func(_ context.Context, _ string) (func(error), error)
	rpcCallFinished   func(ctx context.Context)
}

func (f funcMethodLimiter) RPCCallStarting(ctx context.Context, methodName string, md metadata.MD) (context.Context, error) {
	return f.rpcCallStarting(ctx, methodName, md)
}

func (f funcMethodLimiter) RPCCallProcessing(ctx context.Context, methodName string) (func(error), error) {
	return f.rpcCallProcessing(ctx, methodName)
}

func (f funcMethodLimiter) RPCCallFinished(ctx context.Context) {
	f.rpcCallFinished(ctx)
}
