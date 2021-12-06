package kubernetes

import (
	"io"

	"github.com/go-kit/log"

	"github.com/grafana/dskit/kv/codec"

	"k8s.io/client-go/kubernetes/fake"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	fake_rest "k8s.io/client-go/rest/fake"
)

type fakeClientset struct {
	*fake.Clientset
}

func (c fakeClientset) CoreV1() corev1.CoreV1Interface {
	return fakeCoreV1{c.Clientset.CoreV1()}
}

type fakeCoreV1 struct {
	corev1.CoreV1Interface
}

func (c fakeCoreV1) RESTClient() rest.Interface {
	return &fake_rest.RESTClient{}
}

func NewInMemoryClient(codec codec.Codec, logger log.Logger) (*Client, io.Closer) {
	fakeClientset := fakeClientset{fake.NewSimpleClientset()}

	client, err := newClient(Config{}, codec, logger, nil, func(c *Client) error {
		c.clientset = fakeClientset
		return nil
	})
	if err != nil {
		panic("error generating in memory client: " + err.Error())
	}

	return client, io.NopCloser(nil)
}
