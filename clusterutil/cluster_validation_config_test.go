package clusterutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterValidationProtocolConfigValidate(t *testing.T) {
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
