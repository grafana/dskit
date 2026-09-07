package runtimeconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfigSource(t *testing.T) {
	for _, tc := range []struct {
		name           string
		entry          string
		wantPath       string
		wantParameters sourceParameters
		wantErr        string
	}{
		{
			name:     "plain file",
			entry:    "/etc/overrides.yaml",
			wantPath: "/etc/overrides.yaml",
		},
		{
			name:     "plain URL",
			entry:    "http://config-server/overrides",
			wantPath: "http://config-server/overrides",
		},
		{
			name:           "optional on startup",
			entry:          "http://config-server/overrides;optional-on-startup",
			wantPath:       "http://config-server/overrides",
			wantParameters: sourceParameters{parameterOptionalOnStartup},
		},
		{
			name:           "optional use last value",
			entry:          "/etc/overrides.yaml;optional-use-last-value",
			wantPath:       "/etc/overrides.yaml",
			wantParameters: sourceParameters{parameterOptionalUseLastValue},
		},
		{
			name:     "IPv6 URL keeps its brackets",
			entry:    "http://[::1]:8080/overrides",
			wantPath: "http://[::1]:8080/overrides",
		},
		{
			name:           "IPv6 URL with a parameter",
			entry:          "http://[::1]:8080/overrides;optional-on-startup",
			wantPath:       "http://[::1]:8080/overrides",
			wantParameters: sourceParameters{parameterOptionalOnStartup},
		},
		{
			// A semicolon in the path that is not a known parameter stays part of the URL,
			// matching JDBC-style path parameters.
			name:     "URL path parameter is not a source parameter",
			entry:    "http://config-server/overrides.yaml;jsessionid=ABC",
			wantPath: "http://config-server/overrides.yaml;jsessionid=ABC",
		},
		{
			name:           "URL path parameter then a known parameter",
			entry:          "http://config-server/overrides.yaml;jsessionid=ABC;optional-use-last-value",
			wantPath:       "http://config-server/overrides.yaml;jsessionid=ABC",
			wantParameters: sourceParameters{parameterOptionalUseLastValue},
		},
		{
			// Only the exact parameter suffixes are parameters, so a misspelling is a path and
			// fails later when the source is read.
			name:     "misspelled parameter stays part of the path",
			entry:    "/etc/overrides.yaml;optional",
			wantPath: "/etc/overrides.yaml;optional",
		},
		{
			name:     "short path parameter stays part of the path",
			entry:    "http://config-server/overrides;v2",
			wantPath: "http://config-server/overrides;v2",
		},
		{
			name:     "non-ASCII path parameter stays part of the path",
			entry:    "/etc/overrides.yaml;café",
			wantPath: "/etc/overrides.yaml;café",
		},
		{
			name:     "trailing semicolon stays part of the path",
			entry:    "/etc/overrides.yaml;",
			wantPath: "/etc/overrides.yaml;",
		},
		{
			name:    "conflicting parameters",
			entry:   "/etc/overrides.yaml;optional-on-startup;optional-use-last-value",
			wantErr: `more than one parameter`,
		},
		{
			name:    "conflicting parameters in reverse order",
			entry:   "/etc/overrides.yaml;optional-use-last-value;optional-on-startup",
			wantErr: `more than one parameter`,
		},
		{
			name:    "parameter without a path",
			entry:   ";optional-on-startup",
			wantErr: `has no path`,
		},
		{
			name:    "two parameters without a path",
			entry:   ";optional-on-startup;optional-use-last-value",
			wantErr: `has no path`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs, err := parseConfigSource(tc.entry)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantPath, cs.path)
			assert.Equal(t, tc.wantParameters, cs.parameters)
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
		{parameter: "", onStartup: false, afterStartup: false, keepsLastValue: false},
		{parameter: parameterOptionalOnStartup, onStartup: true, afterStartup: false, keepsLastValue: false},
		{parameter: parameterOptionalUseLastValue, onStartup: true, afterStartup: true, keepsLastValue: true},
	} {
		assert.Equal(t, tc.onStartup, tc.parameter.toleratesFailure(true), "parameter %q on startup", tc.parameter)
		assert.Equal(t, tc.afterStartup, tc.parameter.toleratesFailure(false), "parameter %q after startup", tc.parameter)
		assert.Equal(t, tc.keepsLastValue, tc.parameter.keepsLastValue(), "parameter %q keeps last value", tc.parameter)

		ps := sourceParameters{tc.parameter}
		assert.Equal(t, tc.onStartup, ps.toleratesFailure(true), "parameters %v on startup", ps)
		assert.Equal(t, tc.afterStartup, ps.toleratesFailure(false), "parameters %v after startup", ps)
		assert.Equal(t, tc.keepsLastValue, ps.keepsLastValue(), "parameters %v keeps last value", ps)
	}
}

func TestSourceParametersEmpty(t *testing.T) {
	var none sourceParameters
	assert.False(t, none.toleratesFailure(true))
	assert.False(t, none.toleratesFailure(false))
	assert.False(t, none.keepsLastValue())
}
