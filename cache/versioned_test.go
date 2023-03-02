package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestVersioned(t *testing.T) {
	t.Run("happy case: can store, retrieve and delete", func(t *testing.T) {
		cache := NewMockCache()
		v1 := NewVersioned(cache, 1)
		data := map[string][]byte{"hit": []byte(`data`)}
		v1.StoreAsync(data, time.Minute)
		res := v1.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, data, res)
		err := v1.Delete(context.Background(), "hit")
		require.NoError(t, err)
		res = v1.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, map[string][]uint8{}, res)
	})

	t.Run("different versions use different datasets", func(t *testing.T) {
		cache := NewMockCache()
		v1 := NewVersioned(cache, 1)
		v1Data := map[string][]byte{"hit": []byte(`first version`)}
		v1.StoreAsync(v1Data, time.Minute)
		v2 := NewVersioned(cache, 2)
		v2Data := map[string][]byte{"hit": []byte(`second version`)}
		v2.StoreAsync(v2Data, time.Minute)

		resV1 := v1.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, v1Data, resV1)
		resV2 := v2.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, v2Data, resV2)

		err := v1.Delete(context.Background(), "hit")
		require.NoError(t, err)
		resV1 = v1.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, map[string][]uint8{}, resV1)
		resV2 = v2.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, v2Data, resV2, "v2 cached data should still retain the data")
		err = v2.Delete(context.Background(), "hit")
		require.NoError(t, err)
		resV2 = v2.Fetch(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, map[string][]uint8{}, resV2)
	})
}
