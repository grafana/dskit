package runtimeconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSource(t *testing.T) {
	for _, tc := range []struct {
		name       string
		entry      string
		wantPath   string
		wantPolicy failurePolicy
		wantErr    string
	}{
		{
			name:       "plain file",
			entry:      "/etc/overrides.yaml",
			wantPath:   "/etc/overrides.yaml",
			wantPolicy: failureIsFatal,
		},
		{
			name:       "plain URL",
			entry:      "http://config-server/overrides",
			wantPath:   "http://config-server/overrides",
			wantPolicy: failureIsFatal,
		},
		{
			name:       "optional on startup",
			entry:      "http://config-server/overrides[optional-on-startup]",
			wantPath:   "http://config-server/overrides",
			wantPolicy: failureToleratedOnStartup,
		},
		{
			name:       "optional use last value",
			entry:      "/etc/overrides.yaml[optional-use-last-value]",
			wantPath:   "/etc/overrides.yaml",
			wantPolicy: failureUsesLastValue,
		},
		{
			// Only a trailing option is parsed, so the brackets of an IPv6 host are left alone.
			name:       "IPv6 URL keeps its brackets",
			entry:      "http://[::1]:8080/overrides",
			wantPath:   "http://[::1]:8080/overrides",
			wantPolicy: failureIsFatal,
		},
		{
			name:       "IPv6 URL with an option",
			entry:      "http://[::1]:8080/overrides[optional-on-startup]",
			wantPath:   "http://[::1]:8080/overrides",
			wantPolicy: failureToleratedOnStartup,
		},
		{
			name:    "unknown option",
			entry:   "/etc/overrides.yaml[optional]",
			wantErr: `unknown option "optional"`,
		},
		{
			// The two options contradict each other, and a comma would in any case be split by
			// the comma separated list before it ever reaches this function.
			name:    "two options",
			entry:   "/etc/overrides.yaml[optional-on-startup;optional-use-last-value]",
			wantErr: `unknown option "optional-on-startup;optional-use-last-value"`,
		},
		{
			name:    "empty option",
			entry:   "/etc/overrides.yaml[]",
			wantErr: `unknown option ""`,
		},
		{
			// A bare IPv6 URL with no port or path ends with "]", so it is reported rather than
			// silently treated as a path.
			name:    "closing bracket without an opening one",
			entry:   "/etc/overrides]",
			wantErr: `has no matching "["`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, err := parseSource(tc.entry)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantPath, src.path)
			assert.Equal(t, tc.wantPolicy, src.policy)
		})
	}
}

func TestSourceTolerates(t *testing.T) {
	for _, tc := range []struct {
		policy         failurePolicy
		onStartup      bool
		afterStartup   bool
		keepsLastValue bool
	}{
		{policy: failureIsFatal, onStartup: false, afterStartup: false, keepsLastValue: false},
		{policy: failureToleratedOnStartup, onStartup: true, afterStartup: false, keepsLastValue: false},
		{policy: failureUsesLastValue, onStartup: true, afterStartup: true, keepsLastValue: true},
	} {
		assert.Equal(t, tc.onStartup, tc.policy.tolerates(true), "policy %d on startup", tc.policy)
		assert.Equal(t, tc.afterStartup, tc.policy.tolerates(false), "policy %d after startup", tc.policy)
		assert.Equal(t, tc.keepsLastValue, tc.policy.keepsLastValue(), "policy %d keeps last value", tc.policy)
	}
}
