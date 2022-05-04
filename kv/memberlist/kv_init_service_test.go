package memberlist

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
)

func TestStop(t *testing.T) {
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	kvinit := NewKVInitService(&cfg, nil, &dnsProviderMock{}, prometheus.NewPedanticRegistry())
	require.NoError(t, kvinit.stopping(nil))
}
