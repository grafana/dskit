package middleware

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"

	dskit_log "github.com/grafana/dskit/log"
)

func TestIsDebugEnabled(t *testing.T) {
	t.Run("plain nop logger (no DebugEnabled interface) returns true", func(t *testing.T) {
		require.True(t, isDebugEnabled(log.NewNopLogger()))
	})

	t.Run("bare go-kit level filter (no DebugEnabled interface) returns true", func(t *testing.T) {
		logger := level.NewFilter(log.NewNopLogger(), level.AllowInfo())
		require.True(t, isDebugEnabled(logger))
	})

	t.Run("dskit FilteredLogger with AllowDebug returns true", func(t *testing.T) {
		var lvl dskit_log.Level
		require.NoError(t, lvl.Set("debug"))
		logger := dskit_log.NewFilteredLogger(log.NewNopLogger(), lvl)
		require.True(t, isDebugEnabled(logger))
	})

	t.Run("dskit FilteredLogger with AllowInfo returns false", func(t *testing.T) {
		var lvl dskit_log.Level
		require.NoError(t, lvl.Set("info"))
		logger := dskit_log.NewFilteredLogger(log.NewNopLogger(), lvl)
		require.False(t, isDebugEnabled(logger))
	})

	t.Run("dskit FilteredLogger with AllowWarn returns false", func(t *testing.T) {
		var lvl dskit_log.Level
		require.NoError(t, lvl.Set("warn"))
		logger := dskit_log.NewFilteredLogger(log.NewNopLogger(), lvl)
		require.False(t, isDebugEnabled(logger))
	})

	t.Run("dskit FilteredLogger with AllowError returns false", func(t *testing.T) {
		var lvl dskit_log.Level
		require.NoError(t, lvl.Set("error"))
		logger := dskit_log.NewFilteredLogger(log.NewNopLogger(), lvl)
		require.False(t, isDebugEnabled(logger))
	})

	t.Run("levelFilteredLogger (bench helper) with debugEnabled=false returns false", func(t *testing.T) {
		logger := newLevelFilteredLogger(log.NewNopLogger(), false)
		require.False(t, isDebugEnabled(logger))
	})

	t.Run("levelFilteredLogger (bench helper) with debugEnabled=true returns true", func(t *testing.T) {
		logger := newLevelFilteredLogger(log.NewNopLogger(), true)
		require.True(t, isDebugEnabled(logger))
	})
}
