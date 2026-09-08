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
			name:           "optional keep last value on failure",
			entry:          "/etc/overrides.yaml;optional-keep-last-value-on-failure",
			wantPath:       "/etc/overrides.yaml",
			wantParameters: sourceParameters{parameterOptionalKeepLastValueOnFailure},
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
			entry:          "http://config-server/overrides.yaml;jsessionid=ABC;optional-keep-last-value-on-failure",
			wantPath:       "http://config-server/overrides.yaml;jsessionid=ABC",
			wantParameters: sourceParameters{parameterOptionalKeepLastValueOnFailure},
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
			entry:   "/etc/overrides.yaml;optional-on-startup;optional-keep-last-value-on-failure",
			wantErr: `more than one parameter`,
		},
		{
			name:    "conflicting parameters in reverse order",
			entry:   "/etc/overrides.yaml;optional-keep-last-value-on-failure;optional-on-startup",
			wantErr: `more than one parameter`,
		},
		{
			name:    "parameter without a path",
			entry:   ";optional-on-startup",
			wantErr: `has no path`,
		},
		{
			name:    "two parameters without a path",
			entry:   ";optional-on-startup;optional-keep-last-value-on-failure",
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

func TestAssignSourceIDs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		paths   []string
		wantIDs []string
		wantErr string
	}{
		{
			name:    "file path is used as it is",
			paths:   []string{"/etc/overrides.yaml"},
			wantIDs: []string{"/etc/overrides.yaml"},
		},
		{
			name:    "URL without credentials is used as it is",
			paths:   []string{"http://config-server:8080/overrides.yaml"},
			wantIDs: []string{"http://config-server:8080/overrides.yaml"},
		},
		{
			name:    "userinfo is dropped, including the username",
			paths:   []string{"https://apikey:@config-server/overrides"},
			wantIDs: []string{"https://config-server/overrides"},
		},
		{
			name:    "query and fragment are dropped",
			paths:   []string{"http://config-server/overrides?token=secret#secret"},
			wantIDs: []string{"http://config-server/overrides"},
		},
		{
			name:    "an empty query leaves no trailing question mark",
			paths:   []string{"http://config-server/overrides?"},
			wantIDs: []string{"http://config-server/overrides"},
		},
		{
			name:  "sources that differ only in what is dropped are told apart by their index",
			paths: []string{"http://config-server/overrides?tenant=a", "http://config-server/overrides?tenant=b"},
			wantIDs: []string{
				"http://config-server/overrides#0",
				"http://config-server/overrides#1",
			},
		},
		{
			// Only the entries that collide are indexed, so the common case stays readable.
			name:  "an unambiguous source keeps its path",
			paths: []string{"http://config-server/a?x=1", "http://config-server/a?x=2", "/etc/overrides.yaml"},
			wantIDs: []string{
				"http://config-server/a#0",
				"http://config-server/a#1",
				"/etc/overrides.yaml",
			},
		},
		{
			name:    "the same source twice is told apart by its index",
			paths:   []string{"/etc/overrides.yaml", "/etc/overrides.yaml"},
			wantIDs: []string{"/etc/overrides.yaml#0", "/etc/overrides.yaml#1"},
		},
		{
			name:    "a URL that cannot be parsed is rejected",
			paths:   []string{"http://config-server:not-a-port/overrides"},
			wantErr: "parse runtime config URL",
		},
		{
			// A file path can hold the "#" that the index suffix uses, so the suffix alone
			// cannot always disambiguate.
			name:    "paths that collide even once indexed are rejected",
			paths:   []string{"/etc/o.yaml", "/etc/o.yaml", "/etc/o.yaml#0"},
			wantErr: `both report as "/etc/o.yaml#0"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := make([]configSource, len(tc.paths))
			for i, p := range tc.paths {
				sources[i].path = p
			}

			err := assignSourceIDs(sources)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)

			ids := make([]string, len(sources))
			for i := range sources {
				ids[i] = sources[i].sourceID
			}
			assert.Equal(t, tc.wantIDs, ids)
		})
	}
}

func TestSourceTolerates(t *testing.T) {
	for _, tc := range []struct {
		parameter               sourceParameter
		onStartup               bool
		afterStartup            bool
		keepsLastValueOnFailure bool
	}{
		{parameter: "", onStartup: false, afterStartup: false, keepsLastValueOnFailure: false},
		{parameter: parameterOptionalOnStartup, onStartup: true, afterStartup: false, keepsLastValueOnFailure: false},
		{parameter: parameterOptionalKeepLastValueOnFailure, onStartup: true, afterStartup: true, keepsLastValueOnFailure: true},
	} {
		assert.Equal(t, tc.onStartup, tc.parameter.toleratesFailure(true), "parameter %q on startup", tc.parameter)
		assert.Equal(t, tc.afterStartup, tc.parameter.toleratesFailure(false), "parameter %q after startup", tc.parameter)
		assert.Equal(t, tc.keepsLastValueOnFailure, tc.parameter.keepsLastValueOnFailure(), "parameter %q keeps last value on failure", tc.parameter)

		ps := sourceParameters{tc.parameter}
		assert.Equal(t, tc.onStartup, ps.toleratesFailure(true), "parameters %v on startup", ps)
		assert.Equal(t, tc.afterStartup, ps.toleratesFailure(false), "parameters %v after startup", ps)
		assert.Equal(t, tc.keepsLastValueOnFailure, ps.keepsLastValueOnFailure(), "parameters %v keeps last value on failure", ps)
	}
}

func TestSourceParametersEmpty(t *testing.T) {
	var none sourceParameters
	assert.False(t, none.toleratesFailure(true))
	assert.False(t, none.toleratesFailure(false))
	assert.False(t, none.keepsLastValueOnFailure())
}
