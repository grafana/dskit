package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersioned(t *testing.T) {
	t.Run("happy case: can store, retrieve and delete", func(t *testing.T) {
		cache := NewMockCache()
		v1 := NewVersioned(cache, 1)
		data := map[string][]byte{"hit": []byte(`data`)}
		v1.SetMultiAsync(data, time.Minute)
		res := v1.GetMulti(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, data, res)
		err := v1.Delete(context.Background(), "hit")
		require.NoError(t, err)
		res = v1.GetMulti(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, map[string][]uint8{}, res)
	})

	t.Run("different versions use different datasets", func(t *testing.T) {
		cache := NewMockCache()
		v1 := NewVersioned(cache, 1)
		v1Data := map[string][]byte{"hit": []byte(`first version`)}
		v1.SetMultiAsync(v1Data, time.Minute)
		v2 := NewVersioned(cache, 2)
		v2Data := map[string][]byte{"hit": []byte(`second version`)}
		v2.SetMultiAsync(v2Data, time.Minute)

		resV1 := v1.GetMulti(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, v1Data, resV1)
		resV2 := v2.GetMulti(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, v2Data, resV2)

		err := v1.Delete(context.Background(), "hit")
		require.NoError(t, err)
		resV1 = v1.GetMulti(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, map[string][]uint8{}, resV1)
		resV2 = v2.GetMulti(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, v2Data, resV2, "v2 cached data should still retain the data")
		err = v2.Delete(context.Background(), "hit")
		require.NoError(t, err)
		resV2 = v2.GetMulti(context.Background(), []string{"hit", "miss"})
		assert.Equal(t, map[string][]uint8{}, resV2)
	})

	t.Run("GetMultiWithError works correctly", func(t *testing.T) {
		cache := NewMockCache()
		v1 := NewVersioned(cache, 1)
		data := map[string][]byte{"hit": []byte(`data`)}
		v1.SetMultiAsync(data, time.Minute)
		res, err := v1.GetMultiWithError(context.Background(), []string{"hit", "miss"})
		require.NoError(t, err)
		assert.Equal(t, data, res)
	})

	t.Run("GetMultiWithError propagates backend errors", func(t *testing.T) {
		backendErr := errors.New("backend error")
		backend := NewErroringMockCache(backendErr)
		v1 := NewVersioned(backend, 1)

		result, err := v1.GetMultiWithError(context.Background(), []string{"key"})
		assert.Empty(t, result)
		assert.ErrorIs(t, err, backendErr)
	})
}
