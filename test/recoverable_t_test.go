package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverableT(t *testing.T) {
	t.Run("FailNow marks test as failed", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)
		defer rt.Recover()

		rt.FailNow()

		assert.True(t, inner.failed, "expected underlying TB to be marked as failed")
	})

	t.Run("Fatal marks test as failed", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)
		defer rt.Recover()

		rt.Fatal("something went wrong")

		assert.True(t, inner.failed, "expected underlying TB to be marked as failed")
		assert.Contains(t, inner.lastError, "something went wrong")
	})

	t.Run("Fatalf marks test as failed", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)
		defer rt.Recover()

		rt.Fatalf("value is %d", 42)

		assert.True(t, inner.failed, "expected underlying TB to be marked as failed")
		assert.Contains(t, inner.lastError, "value is 42")
	})

	t.Run("Recover catches FailNow", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)

		func() {
			defer rt.Recover()
			rt.FailNow()
			t.Fatal("should not reach here")
		}()

		assert.True(t, inner.failed)
	})

	t.Run("Recover re-raises non-sentinel panic", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)

		assert.PanicsWithValue(t, "real panic", func() {
			defer rt.Recover()
			panic("real panic")
		})
	})

	t.Run("RecoverError returns ErrTestFailed", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)

		err := func() (retErr error) {
			defer rt.RecoverError(&retErr)
			rt.FailNow()
			return nil
		}()

		require.ErrorIs(t, err, ErrTestFailed)
		assert.True(t, inner.failed)
	})

	t.Run("RecoverError returns nil on success", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)

		err := func() (retErr error) {
			defer rt.RecoverError(&retErr)
			return nil
		}()

		require.NoError(t, err)
		assert.False(t, inner.failed)
	})

	t.Run("RecoverError re-raises non-sentinel panic", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)

		assert.PanicsWithValue(t, "real panic", func() {
			defer func(retErr *error) {
				rt.RecoverError(retErr)
			}(new(error))
			panic("real panic")
		})
	})

	t.Run("require.Equal stops execution on failure", func(t *testing.T) {
		inner := &mockTB{}
		rt := NewRecoverableT(inner)

		continued := false
		func() {
			defer rt.Recover()
			require.Equal(rt, 1, 2, "should fail")
			continued = true
		}()

		assert.True(t, inner.failed, "expected test to be marked as failed")
		assert.False(t, continued, "expected execution to stop after require.Equal failure")
	})
}

// mockTB is a minimal testing.TB implementation for unit testing RecoverableT.
type mockTB struct {
	testing.TB
	failed    bool
	lastError string
}

func (m *mockTB) Fail()        { m.failed = true }
func (m *mockTB) Helper()      {}
func (m *mockTB) Name() string { return "mock" }

func (m *mockTB) Errorf(format string, args ...any) {
	m.failed = true
	m.lastError = fmt.Sprintf(format, args...)
}
