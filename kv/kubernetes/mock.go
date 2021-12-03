package kubernetes

import (
	"io"

	"github.com/go-kit/log"

	"github.com/grafana/dskit/kv/codec"

	"k8s.io/client-go/kubernetes/fake"
)

func NewInMemoryClient(codec codec.Codec, logger log.Logger) (*Client, io.Closer) {
	fakeClientset := fake.NewSimpleClientset()

	client, err := newClient(&Config{}, codec, logger, nil, func(c *Client) error {
		c.clientset = fakeClientset
		return nil
	})
	if err != nil {
		panic("error generating in memory client: " + err.Error())
	}

	return client, io.NopCloser(nil)
}
