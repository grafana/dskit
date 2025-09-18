package clusterutil

import (
	"flag"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterValidationConfig_RegisteredFlags(t *testing.T) {
	cfg := ClusterValidationConfig{}
	// Before we track registered flags, no registered flags is returned.
	require.Empty(t, cfg.RegisteredFlags())

	fs := flag.NewFlagSet("test", flag.PanicOnError)
	cfg.RegisterFlagsWithPrefix("prefix", fs)

	// After we track registered flags, both label and labels flags are returned.
	registeredFlags := cfg.RegisteredFlags()
	require.NotEmpty(t, registeredFlags)
	require.Equal(t, "prefix", registeredFlags.Prefix)
	require.Len(t, registeredFlags.Flags, 2)
	_, ok := registeredFlags.Flags["label"]
	require.True(t, ok)
	_, ok = registeredFlags.Flags["labels"]
	require.True(t, ok)
}

func TestClusterValidationProtocolConfig_Validate(t *testing.T) {
	testCases := map[string]struct {
		labels         []string
		enabled        bool
		softValidation bool
		expectedErr    error
	}{
		"soft validation cannot be done if cluster validation labels are not set": {
			softValidation: true,
			expectedErr:    fmt.Errorf("testProtocol: validation cannot be enabled if cluster validation labels are not configured"),
		},
		"cluster validation cannot be done if cluster validation labels are not set": {
			enabled:     true,
			expectedErr: fmt.Errorf("testProtocol: validation cannot be enabled if cluster validation labels are not configured"),
		},
		"cluster validation and soft validation can be disabled if cluster validation labels are not set": {
			labels:         []string{},
			enabled:        false,
			softValidation: false,
		},
		"cluster validation and soft validation can be disabled if cluster validation labels are set": {
			labels:         []string{"my-cluster"},
			enabled:        false,
			softValidation: false,
		},
		"soft validation cannot be enabled if cluster validation is disabled": {
			labels:         []string{"my-cluster"},
			enabled:        false,
			softValidation: true,
			expectedErr:    fmt.Errorf("testProtocol: soft validation can be enabled only if cluster validation is enabled"),
		},
		"soft validation can be disabled if cluster validation is enabled": {
			labels:         []string{"my-cluster"},
			enabled:        true,
			softValidation: false,
		},
		"cluster validation and soft validation can be enabled at the same time": {
			labels:         []string{"my-cluster"},
			enabled:        true,
			softValidation: true,
		},
		"multiple cluster labels are supported": {
			labels:         []string{"cluster-a", "cluster-b", "cluster-c"},
			enabled:        true,
			softValidation: false,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			testProtocolCfg := ClusterValidationProtocolConfig{
				Enabled:        testCase.enabled,
				SoftValidation: testCase.softValidation,
			}
			err := testProtocolCfg.Validate("testProtocol", testCase.labels)
			require.Equal(t, testCase.expectedErr, err)
		})
	}
}

func TestServerClusterValidationConfig_RegisteredFlags(t *testing.T) {
	var cfg = ServerClusterValidationConfig{}
	// Before we track registered flags, no registered flags is returned.
	require.Empty(t, cfg.RegisteredFlags())

	fs := flag.NewFlagSet("test", flag.PanicOnError)
	cfg.RegisterFlagsWithPrefix("server.cluster-validation.", fs)

	// After we track registered flags, label, labels, grpc.enabled and grpc.soft-validation flags are returned.
	registeredFlags := cfg.RegisteredFlags()
	require.NotEmpty(t, registeredFlags)
	require.Equal(t, "server.cluster-validation.", registeredFlags.Prefix)
	expectedFlags := []string{"label", "labels", "grpc.enabled", "grpc.soft-validation", "http.enabled", "http.soft-validation", "http.excluded-paths", "http.excluded-user-agents"}
	require.ElementsMatch(t, expectedFlags, slices.Collect(maps.Keys(registeredFlags.Flags)))
}

func TestClusterValidationConfig_GetEffectiveLabels(t *testing.T) {
	testCases := map[string]struct {
		label          string
		labels         []string
		expectedLabels []string
	}{
		"empty config returns nil slice": {
			label:          "",
			labels:         nil,
			expectedLabels: nil,
		},
		"only deprecated label set": {
			label:          "cluster-a",
			labels:         nil,
			expectedLabels: []string{"cluster-a"},
		},
		"only new labels set": {
			label:          "",
			labels:         []string{"cluster-a", "cluster-b"},
			expectedLabels: []string{"cluster-a", "cluster-b"},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			cfg := ClusterValidationConfig{
				Label:  testCase.label,
				Labels: testCase.labels,
			}
			effective := cfg.GetEffectiveLabels()
			require.Equal(t, testCase.expectedLabels, effective)
		})
	}
}

func TestClusterValidationConfig_Validate(t *testing.T) {
	testCases := map[string]struct {
		label       string
		labels      []string
		expectError bool
		errorMsg    string
	}{
		"empty config is valid": {
			label:       "",
			labels:      nil,
			expectError: false,
		},
		"only deprecated label is valid": {
			label:       "cluster-a",
			labels:      nil,
			expectError: false,
		},
		"only new labels is valid": {
			label:       "",
			labels:      []string{"cluster-a", "cluster-b"},
			expectError: false,
		},
		"both label and labels set is invalid": {
			label:       "cluster-a",
			labels:      []string{"cluster-b"},
			expectError: true,
			errorMsg:    "cluster validation label and labels cannot both be set - use labels instead of the deprecated label flag",
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			cfg := ClusterValidationConfig{
				Label:  testCase.label,
				Labels: testCase.labels,
			}
			err := cfg.Validate()
			if testCase.expectError {
				require.Error(t, err)
				require.Equal(t, testCase.errorMsg, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
