package kubernetes

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

type testCodec struct {
}

func (testCodec) Decode(in []byte) (interface{}, error) {
	return string(in), nil
}

func (testCodec) Encode(in interface{}) ([]byte, error) {
	return []byte(in.(string)), nil
}

func (testCodec) CodecID() string {
	return "test"
}

func newTestClient(t testing.TB) *Client {
	var (
		logger = log.NewNopLogger()
	)

	if testing.Verbose() {
		logger = log.NewLogfmtLogger(os.Stderr)
	}

	// use a real Kubernetes client if both environment DSKIT_TEST_KUBERNETES and KUBECONFIG are set
	if os.Getenv("DSKIT_TEST_KUBERNETES") != "" && os.Getenv("KUBECONFIG") != "" {
		t.Logf("connecting to real Kubernetes cluster")
		client, err := NewClient(
			Config{ConfigMapName: "test-integration-" + randStringRunes(8)},
			testCodec{},
			logger,
			nil,
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			if err := client.clientset.CoreV1().ConfigMaps(client.namespace).Delete(context.Background(), client.name, metav1.DeleteOptions{}); err != nil {
				t.Logf("unable to delete config map: %v", err)
			}
		})
		return client
	}

	// otherwise use in memory client
	client, closer := NewInMemoryClient(
		testCodec{},
		logger,
	)
	t.Cleanup(func() {
		closer.Close()
		close(client.stopCh)
	})

	return client
}

func Test_Integration_Simple(t *testing.T) {
	c := newTestClient(t)

	keys, err := c.List(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, keys)

	value, err := c.Get(context.Background(), "not-exists")
	require.NoError(t, err)
	assert.Nil(t, value)

	require.NoError(t, c.CAS(context.Background(), "/test", func(_ interface{}) (out interface{}, retry bool, err error) {
		out = "test"
		retry = false
		return
	}))

	require.NoError(t, c.CAS(context.Background(), "/test", func(old interface{}) (out interface{}, retry bool, err error) {
		assert.Equal(t, "test", old)
		out = nil
		retry = false
		return
	}))

	keys, err = c.List(context.TODO(), "/test")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"/test"}, keys)

	value, err = c.Get(context.TODO(), "/test")
	require.NoError(t, err)
	assert.Equal(t, "test", value)
}

func Test_Delete(t *testing.T) {
	t.Run("happy flow", func(t *testing.T) {
		c := newTestClient(t)

		require.NoError(t, c.CAS(context.Background(), "/test", func(_ interface{}) (out interface{}, retry bool, err error) {
			out = "test"
			retry = false
			return
		}))

		require.NoError(t, c.Delete(context.Background(), "/test"))

		keys, err := c.List(context.TODO(), "/test")
		require.NoError(t, err)
		assert.Empty(t, keys)

		value, err := c.Get(context.TODO(), "/test")
		assert.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("deleting non-existent key also works", func(t *testing.T) {
		c := newTestClient(t)

		require.NoError(t, c.Delete(context.Background(), "/test"))

		keys, err := c.List(context.TODO(), "/test")
		require.NoError(t, err)
		assert.Empty(t, keys)

		value, err := c.Get(context.TODO(), "/test")
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}
