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

func newClient(t testing.TB) *Client {
	logger := log.NewNopLogger()
	if testing.Verbose() {
		logger = log.NewLogfmtLogger(os.Stderr)
	}
	c, err := NewClient(
		&Config{Name: "test-integration-" + randStringRunes(8)},
		testCodec{},
		logger,
		nil,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		// TODO: Implement cleanup of config map
		if err := c.client.CoreV1().ConfigMaps(c.namespace).Delete(context.Background(), c.name, metav1.DeleteOptions{}); err != nil {
			t.Logf("unable to delete config map: %v", err)
		}
	})

	return c
}

func Test_Integration(t *testing.T) {
	c := newClient(t)

	keys, err := c.List(context.Background(), "")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{}, keys)

	value, err := c.Get(context.Background(), "not-exists")
	require.NoError(t, err)
	assert.Nil(t, value)

	require.NoError(t, c.CAS(context.Background(), "/test", func(_ interface{}) (out interface{}, retry bool, err error) {
		out = "test"
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
