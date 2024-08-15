package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	e2 "github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestNilServiceFailureWatcher(t *testing.T) {
	var w *FailureWatcher

	// prove it doesn't fail, but returns nil channel.
	require.Nil(t, w.Chan())

	// Ensure WatchService() panics.
	require.Panics(t, func() {
		w.WatchService(NewIdleService(nil, nil))
	})

	// Ensure WatchManager() panics.
	m, err := NewManager(NewIdleService(nil, nil))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, StopManagerAndAwaitStopped(context.Background(), m))
	})

	require.Panics(t, func() {
		w.WatchManager(m)
	})

	// Closing nil watcher doesn't panic.
	w.Close()
}

func TestServiceFailureWatcher(t *testing.T) {
	w := NewFailureWatcher()

	err := errors.New("this error doesn't end with dot")

	failing := NewBasicService(nil, nil, func(_ error) error {
		return err
	})

	w.WatchService(failing)

	require.NoError(t, failing.StartAsync(context.Background()))

	e := <-w.Chan()
	require.NotNil(t, e)
	require.Equal(t, err, e2.Cause(e))
}

func TestServiceFailureWatcherClose(t *testing.T) {
	s1 := serviceThatDoesntDoAnything()
	s2 := serviceThatDoesntDoAnything()
	m, err := NewManager(s1, s2)
	require.NoError(t, err)

	s3, errorsS3 := errorReturningService()
	s4, errorsS4 := errorReturningService()

	require.NoError(t, StartManagerAndAwaitHealthy(context.Background(), m))
	require.NoError(t, StartAndAwaitRunning(context.Background(), s3))
	require.NoError(t, StartAndAwaitRunning(context.Background(), s4))

	// All goroutines created until now are unrelated to failure watcher.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent(), goleak.Cleanup(func(_ int) {
		require.NoError(t, StopManagerAndAwaitStopped(context.Background(), m))
		// These services are expected to errors.
		require.Error(t, StopAndAwaitTerminated(context.Background(), s3))
		require.Error(t, StopAndAwaitTerminated(context.Background(), s4))

		goleak.VerifyNone(t)
	}))

	// Watch manager and both services
	w := NewFailureWatcher()
	w.WatchManager(m)
	w.WatchService(s3)
	w.WatchService(s4)

	ch := w.Chan()

	// Verify that we receive error from s3.
	err = fmt.Errorf("service3 error")
	errorsS3 <- err

	require.ErrorIs(t, <-ch, err)

	// After closing failure watcher, we don't receive any more errors.
	w.Close()
	require.Nil(t, <-ch)

	// Even sending error from service 4 doesn't make it to failure watcher.
	errorsS4 <- fmt.Errorf("service4 error")
	require.Nil(t, <-ch)

	// Since watcher is now closed, it cannot be used to watch services or managers.
	require.Panics(t, func() {
		w.WatchService(s3)
	})
	require.Panics(t, func() {
		w.WatchManager(m)
	})
}

// Creates service which will return first error passed to the channel.
func errorReturningService() (*BasicService, chan<- error) {
	errCh := make(chan error)
	return NewBasicService(nil, func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case e := <-errCh:
				return e
			}
		}
	}, nil), errCh
}
