package runtimeconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func BenchmarkManagerLoadConfig(b *testing.B) {
	dir := b.TempDir()

	yamlData, err := yaml.Marshal(generateOverrides(1000))
	require.NoError(b, err)
	jsonData, err := json.Marshal(generateOverrides(15000))
	require.NoError(b, err)

	yamlPath := filepath.Join(dir, "small.yaml")
	jsonPath := filepath.Join(dir, "big.json")
	require.NoError(b, os.WriteFile(yamlPath, yamlData, 0600))
	require.NoError(b, os.WriteFile(jsonPath, jsonData, 0600))

	cfg := Config{
		LoadPath: []string{yamlPath, jsonPath},
		Loader:   benchLoader,
	}
	m, err := New(cfg, "bench", prometheus.NewPedanticRegistry(), log.NewNopLogger())
	require.NoError(b, err)

	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		// Reset the per-file hash cache so every iteration performs a full
		// reload instead of short-circuiting on unchanged hashes.
		m.fileHashes = nil
		require.NoError(b, m.loadConfig(ctx))
	}
}

type benchConfig struct {
	IngestionRate      float64           `yaml:"ingestion_rate" json:"ingestion_rate"`
	IngestionBurstSize int               `yaml:"ingestion_burst_size" json:"ingestion_burst_size"`
	MaxGlobalSeries    int               `yaml:"max_global_series_per_user" json:"max_global_series_per_user"`
	MaxLabelNames      int               `yaml:"max_label_names_per_series" json:"max_label_names_per_series"`
	Enabled            bool              `yaml:"enabled" json:"enabled"`
	DisplayName        string            `yaml:"display_name" json:"display_name"`
	Labels             map[string]string `yaml:"labels" json:"labels"`
}

type benchOverrides struct {
	Overrides map[string]benchConfig `yaml:"overrides" json:"overrides"`
}

func newBenchConfig(i int) benchConfig {
	return benchConfig{
		IngestionRate:      float64(10000 + i),
		IngestionBurstSize: 200000 + i,
		MaxGlobalSeries:    1500000 + i,
		MaxLabelNames:      30 + (i % 10),
		Enabled:            i%2 == 0,
		DisplayName:        fmt.Sprintf("Tenant number %08d with a reasonably long display name", i),
		Labels: map[string]string{
			"team":        fmt.Sprintf("team-%04d", i%128),
			"environment": []string{"dev", "staging", "prod"}[i%3],
			"region":      []string{"us-east-1", "us-west-2", "eu-west-1"}[i%3],
			"cost_center": fmt.Sprintf("cc-%06d", i),
		},
	}
}

func generateOverrides(numTenants int) benchOverrides {
	o := benchOverrides{Overrides: make(map[string]benchConfig, numTenants)}
	for i := 0; i < numTenants; i++ {
		o.Overrides[fmt.Sprintf("tenant-%08d", i)] = newBenchConfig(i)
	}
	return o
}

func benchLoader(r io.Reader) (interface{}, error) {
	o := &benchOverrides{}
	if err := yaml.NewDecoder(r).Decode(o); err != nil {
		return nil, err
	}
	return o, nil
}
