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
			entry:      "http://config-server/overrides;optional-on-startup",
			wantPath:   "http://config-server/overrides",
			wantPolicy: failureToleratedOnStartup,
		},
		{
			name:       "optional use last value",
			entry:      "/etc/overrides.yaml;optional-use-last-value",
			wantPath:   "/etc/overrides.yaml",
			wantPolicy: failureUsesLastValue,
		},
		{
			name:       "IPv6 URL keeps its brackets",
			entry:      "http://[::1]:8080/overrides",
			wantPath:   "http://[::1]:8080/overrides",
			wantPolicy: failureIsFatal,
		},
		{
			name:       "IPv6 URL with an option",
			entry:      "http://[::1]:8080/overrides;optional-on-startup",
			wantPath:   "http://[::1]:8080/overrides",
			wantPolicy: failureToleratedOnStartup,
		},
		{
			// A semicolon in the path that is not a known option stays part of the URL,
			// matching JDBC-style path parameters.
			name:       "URL path parameter is not an option",
			entry:      "http://config-server/overrides.yaml;jsessionid=ABC",
			wantPath:   "http://config-server/overrides.yaml;jsessionid=ABC",
			wantPolicy: failureIsFatal,
		},
		{
			name:       "URL path parameter then a known option",
			entry:      "http://config-server/overrides.yaml;jsessionid=ABC;optional-use-last-value",
			wantPath:   "http://config-server/overrides.yaml;jsessionid=ABC",
			wantPolicy: failureUsesLastValue,
		},
		{
			// Only the exact option suffixes are options, so a misspelling is a path and
			// fails later when the source is read.
			name:       "misspelled option stays part of the path",
			entry:      "/etc/overrides.yaml;optional",
			wantPath:   "/etc/overrides.yaml;optional",
			wantPolicy: failureIsFatal,
		},
		{
			name:       "short path parameter stays part of the path",
			entry:      "http://config-server/overrides;v2",
			wantPath:   "http://config-server/overrides;v2",
			wantPolicy: failureIsFatal,
		},
		{
			name:       "non-ASCII path parameter stays part of the path",
			entry:      "/etc/overrides.yaml;café",
			wantPath:   "/etc/overrides.yaml;café",
			wantPolicy: failureIsFatal,
		},
		{
			name:       "trailing semicolon stays part of the path",
			entry:      "/etc/overrides.yaml;",
			wantPath:   "/etc/overrides.yaml;",
			wantPolicy: failureIsFatal,
		},
		{
			name:    "two options",
			entry:   "/etc/overrides.yaml;optional-on-startup;optional-use-last-value",
			wantErr: `more than one option`,
		},
		{
			name:    "option without a path",
			entry:   ";optional-on-startup",
			wantErr: `has no path`,
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
