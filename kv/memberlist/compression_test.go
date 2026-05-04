// SPDX-License-Identifier: Apache-2.0
// Provenance-includes-location: https://github.com/grafana/dskit/

package memberlist

import (
	"flag"
	"testing"

	"github.com/hashicorp/go-metrics"
	metricsprometheus "github.com/hashicorp/go-metrics/prometheus"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestKVConfig_CompressionAlgorithm_Flag(t *testing.T) {
	t.Run("default is lzw", func(t *testing.T) {
		var cfg KVConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs)
		require.NoError(t, fs.Parse(nil))
		require.Equal(t, string(memberlist.CompressionAlgorithmLZW), cfg.CompressionAlgorithm)
	})

	t.Run("explicit snappy", func(t *testing.T) {
		var cfg KVConfig
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(fs)
		require.NoError(t, fs.Parse([]string{"-memberlist.compression-algorithm", "snappy"}))
		require.Equal(t, string(memberlist.CompressionAlgorithmSnappy), cfg.CompressionAlgorithm)
	})
}

// TestPrometheusMetricName_CompressDecoded locks down the name of the new
// memberlist.compress.decoded counter as it surfaces on Prometheus, so a
// future change to dskit's metrics-bridge config (e.g., flipping
// EnableTypePrefix) doesn't silently break operator dashboards or
// integration-test assertions that pin the name.
//
// The bridge config below mirrors createAndRegisterMetrics in metrics.go.
// We don't spin up a *KV — only the metrics layer is under test.
func TestPrometheusMetricName_CompressDecoded(t *testing.T) {
	reg := prometheus.NewRegistry()
	sink, err := metricsprometheus.NewPrometheusSinkFrom(metricsprometheus.PrometheusOpts{
		Expiration: 0,
		Registerer: reg,
	})
	require.NoError(t, err)

	cfg := metrics.DefaultConfig("")
	cfg.EnableHostname = false
	cfg.EnableHostnameLabel = false
	cfg.EnableServiceLabel = false
	cfg.EnableRuntimeMetrics = false
	cfg.EnableTypePrefix = true

	m, err := metrics.New(cfg, sink)
	require.NoError(t, err)

	m.IncrCounterWithLabels(
		[]string{"memberlist", "compress", "decoded"}, 1,
		[]metrics.Label{{Name: "algo", Value: "snappy"}},
	)

	families, err := reg.Gather()
	require.NoError(t, err)

	var got *dto.MetricFamily
	var names []string
	for _, mf := range families {
		names = append(names, mf.GetName())
		if mf.GetName() == "counter_memberlist_compress_decoded" {
			got = mf
		}
	}
	require.NotNil(t, got, "expected counter_memberlist_compress_decoded; got %v", names)

	require.Len(t, got.GetMetric(), 1)
	labelValues := map[string]string{}
	for _, l := range got.GetMetric()[0].GetLabel() {
		labelValues[l.GetName()] = l.GetValue()
	}
	require.Equal(t, "snappy", labelValues["algo"])
}

func TestKVConfig_CompressionAlgorithm_Validate(t *testing.T) {
	tests := []struct {
		algo    string
		wantErr bool
	}{
		{"", false},
		{"lzw", false},
		{"snappy", false},
		{"zstd", true},
		{"LZW", true},
	}
	for _, tc := range tests {
		t.Run(tc.algo, func(t *testing.T) {
			var cfg KVConfig
			fs := flag.NewFlagSet("test", flag.PanicOnError)
			cfg.RegisterFlags(fs)
			cfg.CompressionAlgorithm = tc.algo

			err := cfg.Validate()
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid memberlist compression algorithm")
				return
			}
			require.NoError(t, err)
		})
	}
}
