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
			expectedErr:    fmt.Errorf("testProtocol: no validation can be enabled if cluster validation label is not configured"),
		},
		"hard validation cannot be done if cluster validation label is not set": {
			enabled:     true,
			expectedErr: fmt.Errorf("testProtocol: no validation can be enabled if cluster validation label is not configured"),
		},
		"soft and hard validation can be disabled if cluster validation label is not set": {
			label:          "",
			enabled:        false,
			softValidation: false,
		},
		"soft and hard validation can be disabled if cluster validation label is set": {
			label:          "my-cluster",
			enabled:        false,
			softValidation: false,
		},
		"only soft validation can be enabled if cluster validation label is set": {
			label:          "my-cluster",
			enabled:        false,
			softValidation: true,
		},
		"only hard validation can be enabled if cluster validation label is set": {
			label:          "my-cluster",
			enabled:        true,
			softValidation: false,
		},
		"soft and hard validation cannot be enabled at the same time": {
			label:          "my-cluster",
			enabled:        true,
			softValidation: true,
			expectedErr:    fmt.Errorf("testProtocol: hard validation and soft validation cannot be enabled at the same time"),
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
