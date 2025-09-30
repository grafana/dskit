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

	// After we track registered flags, only label flag is returned (additional-labels moved to server config).
	registeredFlags := cfg.RegisteredFlags()
	require.NotEmpty(t, registeredFlags)
	require.Equal(t, "prefix", registeredFlags.Prefix)
	require.Len(t, registeredFlags.Flags, 1)
	_, ok := registeredFlags.Flags["label"]
	require.True(t, ok)
}

func TestClusterValidationProtocolConfig_Validate(t *testing.T) {
	testCases := map[string]struct {
		label          string
		enabled        bool
		softValidation bool
		expectedErr    error
	}{
		"soft validation cannot be done if cluster validation label is not set": {
			softValidation: true,
			expectedErr:    fmt.Errorf("testProtocol: validation cannot be enabled if cluster validation label is not configured"),
		},
		"cluster validation cannot be done if cluster validation label is not set": {
			enabled:     true,
			expectedErr: fmt.Errorf("testProtocol: validation cannot be enabled if cluster validation label is not configured"),
		},
		"cluster validation and soft validation can be disabled if cluster validation label is not set": {
			label:          "",
			enabled:        false,
			softValidation: false,
		},
		"cluster validation and soft validation can be disabled if cluster validation label is set": {
			label:          "my-cluster",
			enabled:        false,
			softValidation: false,
		},
		"soft validation cannot be enabled if cluster validation is disabled": {
			label:          "my-cluster",
			enabled:        false,
			softValidation: true,
			expectedErr:    fmt.Errorf("testProtocol: soft validation can be enabled only if cluster validation is enabled"),
		},
		"soft validation can be disabled if cluster validation is enabled": {
			label:          "my-cluster",
			enabled:        true,
			softValidation: false,
		},
		"cluster validation and soft validation can be enabled at the same time": {
			label:          "my-cluster",
			enabled:        true,
			softValidation: true,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			testProtocolCfg := ClusterValidationProtocolConfig{
				Enabled:        testCase.enabled,
				SoftValidation: testCase.softValidation,
			}
			err := testProtocolCfg.Validate("testProtocol", testCase.label)
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

	// After we track registered flags, label, additional-labels, grpc.enabled and grpc.soft-validation flags are returned.
	registeredFlags := cfg.RegisteredFlags()
	require.NotEmpty(t, registeredFlags)
	require.Equal(t, "server.cluster-validation.", registeredFlags.Prefix)
	expectedFlags := []string{"label", "additional-labels", "grpc.enabled", "grpc.soft-validation", "http.enabled", "http.soft-validation", "http.excluded-paths", "http.excluded-user-agents"}
	require.ElementsMatch(t, expectedFlags, slices.Collect(maps.Keys(registeredFlags.Flags)))
}

func TestServerClusterValidationConfig_GetAllowedClusterLabels(t *testing.T) {
	testCases := map[string]struct {
		label            string
		additionalLabels []string
		expectedLabels   []string
	}{
		"empty config returns nil slice": {
			label:            "",
			additionalLabels: nil,
			expectedLabels:   nil,
		},
		"only primary label set": {
			label:            "cluster-a",
			additionalLabels: nil,
			expectedLabels:   []string{"cluster-a"},
		},
		"only additional labels set": {
			label:            "",
			additionalLabels: []string{"cluster-a", "cluster-b"},
			expectedLabels:   []string{"cluster-a", "cluster-b"},
		},
		"both primary label and additional labels set": {
			label:            "primary-cluster",
			additionalLabels: []string{"cluster-a", "cluster-b"},
			expectedLabels:   []string{"primary-cluster", "cluster-a", "cluster-b"},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			cfg := ServerClusterValidationConfig{
				ClusterValidationConfig: ClusterValidationConfig{
					Label: testCase.label,
				},
				AdditionalLabels: testCase.additionalLabels,
			}
			effective := cfg.GetAllowedClusterLabels()
			require.Equal(t, testCase.expectedLabels, effective)
		})
	}
}

func TestServerClusterValidationConfig_Validate(t *testing.T) {
	testCases := map[string]struct {
		label            string
		additionalLabels []string
		expectError      bool
		errorMsg         string
	}{
		"empty config is valid": {
			label:            "",
			additionalLabels: nil,
			expectError:      false,
		},
		"only primary label is valid": {
			label:            "cluster-a",
			additionalLabels: nil,
			expectError:      false,
		},
		"only additional labels without primary label is invalid": {
			label:            "",
			additionalLabels: []string{"cluster-a", "cluster-b"},
			expectError:      true,
			errorMsg:         "additional cluster validation labels require primary label to be set",
		},
		"both primary label and additional labels set is valid": {
			label:            "cluster-a",
			additionalLabels: []string{"cluster-b"},
			expectError:      false,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			cfg := ServerClusterValidationConfig{
				ClusterValidationConfig: ClusterValidationConfig{
					Label: testCase.label,
				},
				AdditionalLabels: testCase.additionalLabels,
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
