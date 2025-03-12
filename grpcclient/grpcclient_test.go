package grpcclient

import (
	"flag"
	"fmt"
	"testing"

	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/middleware"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestConfig(t *testing.T) {
	t.Run("custom compressors", func(t *testing.T) {
		const comp = "custom"
		cfg := Config{
			CustomCompressors: []string{comp},
		}
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlagsWithPrefix("test", fs)
		f := fs.Lookup("test.grpc-compression")
		require.NotNil(t, f)
		require.Equal(t, "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 'custom' and '' (disable compression)", f.Usage)

		t.Run("valid compressor", func(t *testing.T) {
			cfg.GRPCCompression = comp

			require.NoError(t, cfg.Validate())
			opts := cfg.CallOptions()

			var compressorOpt grpc.CompressorCallOption
			for _, o := range opts {
				co, ok := o.(grpc.CompressorCallOption)
				if ok {
					compressorOpt = co
					break
				}
			}
			require.Equal(t, comp, compressorOpt.CompressorType)
		})

		t.Run("invalid compressor", func(t *testing.T) {
			cfg.GRPCCompression = "invalid"

			require.EqualError(t, cfg.Validate(), `unsupported compression type: "invalid"`)
		})
	})
}

func TestDialOptionWithClusterValidation(t *testing.T) {
	inputUnaryInterceptors, _ := Instrument(nil)
	testCases := map[string]struct {
		clusterValidation                     clusterutil.ClusterValidationConfig
		rateLimit                             float64
		inputUnaryInterceptors                []grpc.UnaryClientInterceptor
		expectedUnaryInterceptors             int
		expectedClusterUnaryClientInterceptor bool
	}{
		"if cluster validation label is not set we do not expect ClusterUnaryClientInterceptor": {
			clusterValidation:                     clusterutil.ClusterValidationConfig{},
			inputUnaryInterceptors:                inputUnaryInterceptors,
			expectedUnaryInterceptors:             len(inputUnaryInterceptors),
			expectedClusterUnaryClientInterceptor: false,
		},
		"if cluster validation label is set and there is no input and no implicit UnaryClientInterceptors we expect ClusterUnaryClientInterceptor to be the last one": {
			clusterValidation:                     clusterutil.ClusterValidationConfig{Label: "cluster"},
			inputUnaryInterceptors:                nil,
			expectedUnaryInterceptors:             1,
			expectedClusterUnaryClientInterceptor: true,
		},
		"if cluster validation label is set and there is no input and an implicit UnaryClientInterceptor we expect ClusterUnaryClientInterceptor to be the last one": {
			clusterValidation:      clusterutil.ClusterValidationConfig{Label: "cluster"},
			inputUnaryInterceptors: nil,
			// setting rateLimit creates an implicit UnaryClientInterceptor
			rateLimit:                             10,
			expectedUnaryInterceptors:             2,
			expectedClusterUnaryClientInterceptor: true,
		},
		"if cluster validation label is set and there are input and no implicit UnaryClientInterceptors we expect ClusterUnaryClientInterceptor to be the last one": {
			clusterValidation:                     clusterutil.ClusterValidationConfig{Label: "cluster"},
			inputUnaryInterceptors:                inputUnaryInterceptors,
			expectedUnaryInterceptors:             len(inputUnaryInterceptors) + 1,
			expectedClusterUnaryClientInterceptor: true,
		},
		"if cluster validation label is set and there are input and implicit UnaryClientInterceptors we expect ClusterUnaryClientInterceptor to be the last one": {
			clusterValidation:      clusterutil.ClusterValidationConfig{Label: "cluster"},
			inputUnaryInterceptors: inputUnaryInterceptors,
			// setting rateLimit creates an implicit UnaryClientInterceptor
			rateLimit:                             10,
			expectedUnaryInterceptors:             len(inputUnaryInterceptors) + 2,
			expectedClusterUnaryClientInterceptor: true,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			cfg.ClusterValidation = testCase.clusterValidation
			cfg.RateLimit = testCase.rateLimit
			withChainUnaryInterceptor = func(unaryInterceptors ...grpc.UnaryClientInterceptor) grpc.DialOption {
				require.Len(t, unaryInterceptors, testCase.expectedUnaryInterceptors)
				if cfg.ClusterValidation.Label == "" {
					require.Nil(t, cfg.clusterUnaryClientInterceptor)
				} else {
					require.NotNil(t, cfg.clusterUnaryClientInterceptor)
					lastUnaryInterceptor := unaryInterceptors[len(unaryInterceptors)-1]
					require.Equal(t, fmt.Sprintf("%p", cfg.clusterUnaryClientInterceptor), fmt.Sprintf("%p", lastUnaryInterceptor))
				}
				return grpc.WithChainUnaryInterceptor(unaryInterceptors...)
			}
			cfg.DialOption(testCase.inputUnaryInterceptors, nil, middleware.NoOpInvalidClusterValidationReporter)
		})
	}
}
