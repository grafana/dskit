// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/runutil/runutil_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package runutil

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloseWithLogOnErr(t *testing.T) {
	t.Run("With non-close error", func(t *testing.T) {
		closer := fakeCloser{err: fmt.Errorf("an error")}
		logger := fakeLogger{}

		CloseWithLogOnErr(&logger, closer, "closing failed")

		assert.Equal(t, []interface{}{
			"level", level.WarnValue(), "msg", "detected close error", "err", "closing failed: an error",
		}, logger.keyvals)
	})

	t.Run("With no error", func(t *testing.T) {
		closer := fakeCloser{}
		logger := fakeLogger{}

		CloseWithLogOnErr(&logger, closer, "closing failed")

		assert.Empty(t, logger.keyvals)
	})

	t.Run("With closed error", func(t *testing.T) {
		closer := fakeCloser{err: os.ErrClosed}
		logger := fakeLogger{}

		CloseWithLogOnErr(&logger, closer, "closing failed")

		assert.Empty(t, logger.keyvals)
	})
}

type fakeCloser struct {
	err error
}

func (c fakeCloser) Close() error {
	return c.err
}

type fakeLogger struct {
	keyvals []interface{}
}

func (l *fakeLogger) Log(keyvals ...interface{}) error {
	l.keyvals = keyvals
	return nil
}

type testCloser = fakeCloser

func TestCloseWithErrCapture(t *testing.T) {
	for _, tcase := range []struct {
		err    error
		closer io.Closer

		expectedErrStr string
	}{
		{
			err:            nil,
			closer:         testCloser{err: nil},
			expectedErrStr: "",
		},
		{
			err:            errors.New("test"),
			closer:         testCloser{err: nil},
			expectedErrStr: "test",
		},
		{
			err:            nil,
			closer:         testCloser{err: errors.New("test")},
			expectedErrStr: "close: test",
		},
		{
			err:            errors.New("test"),
			closer:         testCloser{err: errors.New("test")},
			expectedErrStr: "2 errors: test; close: test",
		},
	} {
		t.Run(tcase.expectedErrStr, func(t *testing.T) {
			ret := tcase.err
			CloseWithErrCapture(&ret, tcase.closer, "close")

			if tcase.expectedErrStr == "" {
				assert.NoError(t, ret)
			} else {
				require.Error(t, ret)
				assert.Equal(t, tcase.expectedErrStr, ret.Error())
			}
		})
	}
}

type loggerCapturer struct {
	// WasCalled is true if the Log() function has been called.
	WasCalled bool
}

func (lc *loggerCapturer) Log(keyvals ...interface{}) error {
	lc.WasCalled = true
	return nil
}

type emulatedCloser struct {
	io.Reader

	calls int
}

func (e *emulatedCloser) Close() error {
	e.calls++
	if e.calls == 1 {
		return nil
	}
	if e.calls == 2 {
		return errors.Wrap(os.ErrClosed, "can even be a wrapped one")
	}
	return errors.New("something very bad happened")
}

// newEmulatedCloser returns a ReadCloser with a Close method
// that at first returns success but then returns that
// it has been closed already. After that, it returns that
// something very bad had happened.
func newEmulatedCloser(r io.Reader) io.ReadCloser {
	return &emulatedCloser{Reader: r}
}

func TestCloseMoreThanOnce(t *testing.T) {
	lc := &loggerCapturer{}
	r := newEmulatedCloser(strings.NewReader("somestring"))

	CloseWithLogOnErr(lc, r, "should not be called")
	CloseWithLogOnErr(lc, r, "should not be called")
	assert.False(t, lc.WasCalled)

	CloseWithLogOnErr(lc, r, "should be called")
	assert.True(t, lc.WasCalled)
}
