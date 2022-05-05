package memberlist

import (
	"bytes"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func TestPage(t *testing.T) {
	conf := memberlist.DefaultLANConfig()
	ml, err := memberlist.Create(conf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = ml.Shutdown()
	})

	require.NoError(t, defaultPageTemplate.Execute(&bytes.Buffer{}, StatusPageData{
		Now:           time.Now(),
		Memberlist:    ml,
		SortedMembers: ml.Members(),
		Store:         nil,
		ReceivedMessages: []Message{{
			ID:   10,
			Time: time.Now(),
			Size: 50,
			Pair: KeyValuePair{
				Key:   "hello",
				Value: []byte("world"),
				Codec: "codec",
			},
			Version: 20,
			Changes: []string{"A", "B", "C"},
		}},

		SentMessages: []Message{{
			ID:   10,
			Time: time.Now(),
			Size: 50,
			Pair: KeyValuePair{
				Key:   "hello",
				Value: []byte("world"),
				Codec: "codec",
			},
			Version: 20,
			Changes: []string{"A", "B", "C"},
		}},
	}))
}
