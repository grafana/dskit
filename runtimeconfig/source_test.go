package runtimeconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSource(t *testing.T) {
	for _, tc := range []struct {
		name          string
		entry         string
		wantPath      string
		wantParameter sourceParameter
		wantErr       string
	}{
		{
			name:          "plain file",
			entry:         "/etc/overrides.yaml",
			wantPath:      "/etc/overrides.yaml",
			wantParameter: failureIsFatal,
		},
		{
			name:          "plain URL",
			entry:         "http://config-server/overrides",
			wantPath:      "http://config-server/overrides",
			wantParameter: failureIsFatal,
		},
		{
			name:          "optional on startup",
			entry:         "http://config-server/overrides;optional-on-startup",
			wantPath:      "http://config-server/overrides",
			wantParameter: failureToleratedOnStartup,
		},
		{
			name:          "optional use last value",
			entry:         "/etc/overrides.yaml;optional-use-last-value",
			wantPath:      "/etc/overrides.yaml",
			wantParameter: failureUsesLastValue,
		},
		{
			name:          "IPv6 URL keeps its brackets",
			entry:         "http://[::1]:8080/overrides",
			wantPath:      "http://[::1]:8080/overrides",
			wantParameter: failureIsFatal,
		},
		{
			name:          "IPv6 URL with an option",
			entry:         "http://[::1]:8080/overrides;optional-on-startup",
			wantPath:      "http://[::1]:8080/overrides",
			wantParameter: failureToleratedOnStartup,
		},
		{
			// A semicolon in the path that is not a known option stays part of the URL,
			// matching JDBC-style path parameters.
			name:          "URL path parameter is not an option",
			entry:         "http://config-server/overrides.yaml;jsessionid=ABC",
			wantPath:      "http://config-server/overrides.yaml;jsessionid=ABC",
			wantParameter: failureIsFatal,
		},
		{
			name:          "URL path parameter then a known option",
			entry:         "http://config-server/overrides.yaml;jsessionid=ABC;optional-use-last-value",
			wantPath:      "http://config-server/overrides.yaml;jsessionid=ABC",
			wantParameter: failureUsesLastValue,
		},
		{
			// Only the exact option suffixes are options, so a misspelling is a path and
			// fails later when the source is read.
			name:          "misspelled option stays part of the path",
			entry:         "/etc/overrides.yaml;optional",
			wantPath:      "/etc/overrides.yaml;optional",
			wantParameter: failureIsFatal,
		},
		{
			name:          "short path parameter stays part of the path",
			entry:         "http://config-server/overrides;v2",
			wantPath:      "http://config-server/overrides;v2",
			wantParameter: failureIsFatal,
		},
		{
			name:          "non-ASCII path parameter stays part of the path",
			entry:         "/etc/overrides.yaml;café",
			wantPath:      "/etc/overrides.yaml;café",
			wantParameter: failureIsFatal,
		},
		{
			name:          "trailing semicolon stays part of the path",
			entry:         "/etc/overrides.yaml;",
			wantPath:      "/etc/overrides.yaml;",
			wantParameter: failureIsFatal,
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
			assert.Equal(t, tc.wantParameter, src.parameter)
		})
	}
}

func TestSourceTolerates(t *testing.T) {
	for _, tc := range []struct {
		parameter      sourceParameter
		onStartup      bool
		afterStartup   bool
		keepsLastValue bool
	}{
		{parameter: failureIsFatal, onStartup: false, afterStartup: false, keepsLastValue: false},
		{parameter: failureToleratedOnStartup, onStartup: true, afterStartup: false, keepsLastValue: false},
		{parameter: failureUsesLastValue, onStartup: true, afterStartup: true, keepsLastValue: true},
	} {
		assert.Equal(t, tc.onStartup, tc.parameter.tolerates(true), "parameter %d on startup", tc.parameter)
		assert.Equal(t, tc.afterStartup, tc.parameter.tolerates(false), "parameter %d after startup", tc.parameter)
		assert.Equal(t, tc.keepsLastValue, tc.parameter.keepsLastValue(), "parameter %d keeps last value", tc.parameter)
	}
}
