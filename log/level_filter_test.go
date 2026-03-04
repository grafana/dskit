package log

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"
)

func TestFilteredLogger_DebugEnabled(t *testing.T) {
	tests := []struct {
		level   string
		wantDbg bool
	}{
		{"debug", true},
		{"info", false},
		{"warn", false},
		{"error", false},
	}

	for _, tc := range tests {
		t.Run(tc.level, func(t *testing.T) {
			var lvl Level
			require.NoError(t, lvl.Set(tc.level))
			fl := NewFilteredLogger(log.NewNopLogger(), lvl)
			require.Equal(t, tc.wantDbg, fl.DebugEnabled())
		})
	}
}

func TestFilteredLogger_ActuallyFilters(t *testing.T) {
	// Verify that FilteredLogger actually filters messages using the level filter.
	var lvl Level
	require.NoError(t, lvl.Set("warn"))

	calls := 0
	sink := log.LoggerFunc(func(keyvals ...interface{}) error {
		calls++
		return nil
	})

	fl := NewFilteredLogger(sink, lvl)
	require.False(t, fl.DebugEnabled())

	// Debug should be filtered (level=warn).
	_ = level.Debug(fl).Log("msg", "debug-msg")
	require.Equal(t, 0, calls, "debug should be filtered out")

	// Warn should pass through.
	_ = level.Warn(fl).Log("msg", "warn-msg")
	require.Equal(t, 1, calls, "warn should pass through")
}
