package grpcclient

import (
	"flag"
	"testing"

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
