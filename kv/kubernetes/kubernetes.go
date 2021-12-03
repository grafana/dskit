package kubernetes

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"

	"github.com/grafana/dskit/kv/codec"
)

type Config struct {
	Name string // name of the config map
}

type Client struct {
	logger    log.Logger
	name      string // config map name
	namespace string
	client    *kubernetes.Clientset
	codec     codec.Codec

	indexer  cache.Indexer
	queue    workqueue.RateLimitingInterface
	informer cache.Controller
	stopCh   chan struct{}

	configMapMtx sync.RWMutex
	configMap    *v1.ConfigMap
}

func NewClient(cfg *Config, cod codec.Codec, logger log.Logger, registerer prometheus.Registerer) (*Client, error) {
	var config *rest.Config
	var err error

	client := &Client{
		logger: logger,
		codec:  cod,
		name:   "dskit-ring",
		stopCh: make(chan struct{}),
	}

	if cfg != nil {
		if cfg.Name != "" {
			client.name = cfg.Name
		}
	}

	// if environment variable is set use local kubeconfig otherwise fall back to in-cluster client
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		kubeconfigCfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
			&clientcmd.ConfigOverrides{},
		)
		config, err = kubeconfigCfg.ClientConfig()
		if err != nil {
			return nil, err
		}
		client.namespace, _, err = kubeconfigCfg.Namespace()
		if err != nil {
			return nil, err
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	client.client = clientset

	// check if config already exits
	client.configMapMtx.Lock()
	defer client.configMapMtx.Unlock()
	client.configMap, err = clientset.CoreV1().ConfigMaps(client.namespace).Get(context.Background(), client.name, metav1.GetOptions{})
	if err == nil {
		if err := client.startController(); err != nil {
			return nil, err
		}
		return client, nil
	} else if !errors.IsNotFound(err) {
		return nil, err
	}

	// create a new config map
	client.configMap = &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: client.name,
		},
	}
	client.configMap, err = clientset.CoreV1().ConfigMaps(client.namespace).Create(context.Background(), client.configMap, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	// start controller to watch for changes to the config map
	if err := client.startController(); err != nil {
		return nil, err
	}

	return client, nil

}

func convertKeyToStore(in string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(in))
}

func convertKeyFromStore(in string) (string, error) {
	body, err := base64.RawURLEncoding.DecodeString(in)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// List returns a list of keys under the given prefix. Returned keys will
// include the prefix.
func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	c.configMapMtx.RLock()
	cm := c.configMap
	c.configMapMtx.RUnlock()

	var keys []string

	for keyStore := range cm.BinaryData {
		key, err := convertKeyFromStore(keyStore)
		if err != nil {
			c.logger.Log(fmt.Sprintf("unable to decode key '%s'", keyStore))
			continue
		}
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// Get a specific key.  Will use a codec to deserialise key to appropriate type.
// If the key does not exist, Get will return nil and no error.
func (c *Client) Get(ctx context.Context, key string) (interface{}, error) {
	c.configMapMtx.RLock()
	cm := c.configMap
	c.configMapMtx.RUnlock()

	value, ok := cm.BinaryData[convertKeyToStore(key)]
	if !ok {
		return nil, nil
	}
	return value, nil
}

// Delete a specific key. Deletions are best-effort and no error will
// be returned if the key does not exist.
func (c *Client) Delete(ctx context.Context, key string) error {
	return fmt.Errorf("unimplemented")
}

// CAS stands for Compare-And-Swap.  Will call provided callback f with the
// current value of the key and allow callback to return a different value.
// Will then attempt to atomically swap the current value for the new value.
// If that doesn't succeed will try again - callback will be called again
// with new value etc.  Guarantees that only a single concurrent CAS
// succeeds.  Callback can return nil to indicate it is happy with existing
// value.
func (c *Client) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	var (
		intermediate interface{}
		err          error

		cm *v1.ConfigMap
	)

	c.configMapMtx.RLock()
	cm = c.configMap
	c.configMapMtx.RUnlock()

	storedValue, ok := cm.BinaryData[convertKeyToStore(key)]
	if ok && storedValue != nil {
		intermediate, err = c.codec.Decode(storedValue)
		if err != nil {
			return err
		}
	}

	intermediate, _, err = f(intermediate)
	if err != nil {
		return err
	}

	if intermediate == nil {
		return nil
	}

	encoded, err := c.codec.Encode(intermediate)
	if err != nil {
		return err
	}

	newCM := cm.DeepCopy()
	if newCM.BinaryData == nil {
		newCM.BinaryData = make(map[string][]byte)
	}
	newCM.BinaryData[convertKeyToStore(key)] = encoded

	updatedCM, err := c.client.CoreV1().ConfigMaps(c.namespace).Update(ctx, newCM, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	c.configMapMtx.Lock()
	c.configMap = updatedCM
	c.configMapMtx.Unlock()

	return nil
}

// WatchKey calls f whenever the value stored under key changes.
func (c *Client) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	panic("implement me")
}

// WatchPrefix calls f whenever any value stored under prefix changes.
func (c *Client) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	panic("implement me")
}
