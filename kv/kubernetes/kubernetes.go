package kubernetes

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/grafana/dskit/kv/codec"
	watch "github.com/grafana/dskit/kv/internal/watcher"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

const (
	hashPrefix    = "__hash_"
	maxCASRetries = 100
)

type Config struct {
	ConfigMapName    string `yaml:"config_map_name"`
	MetricsNamespace string `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
// If prefix is not an empty string it should end with a period.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.ConfigMapName, prefix+"kubernetes.config-map-name", "dskit-ring", "Name of kubernetes configmap to use for KV store.")
}

type Client struct {
	logger    log.Logger
	name      string // config map name
	namespace string
	clientset kubernetes.Interface
	codec     codec.Codec

	indexer  cache.Indexer
	queue    workqueue.RateLimitingInterface
	informer cache.Controller
	stopCh   chan struct{}

	configMapMtx sync.RWMutex
	configMap    *v1.ConfigMap

	watcher *watch.Watcher

	metrics *metrics
}

func realClientGenerator(c *Client) error {
	var (
		config *rest.Config
		err    error
	)

	// if environment variable is set use local kubeconfig otherwise fall back to in-cluster client
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		kubeconfigCfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
			&clientcmd.ConfigOverrides{},
		)
		config, err = kubeconfigCfg.ClientConfig()
		if err != nil {
			return err
		}
		c.namespace, _, err = kubeconfigCfg.Namespace()
		if err != nil {
			return err
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			return err
		}

		nsBytes, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err != nil {
			return err
		}
		c.namespace = string(nsBytes)
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	c.clientset = clientset
	return nil
}

func NewClient(cfg Config, cod codec.Codec, logger log.Logger, registerer prometheus.Registerer) (*Client, error) {
	return newClient(cfg, cod, logger, registerer, realClientGenerator)
}

func newClient(cfg Config, cod codec.Codec, logger log.Logger, registerer prometheus.Registerer, clientGenerator func(*Client) error) (*Client, error) {
	var err error

	client := &Client{
		logger:  logger,
		codec:   cod,
		name:    cfg.ConfigMapName,
		stopCh:  make(chan struct{}),
		watcher: watch.NewWatcher(logger),
		metrics: newMetrics(cfg.MetricsNamespace, registerer),
	}

	// creates the clientset
	if err := clientGenerator(client); err != nil {
		return nil, err
	}

	// check if config already exits
	client.configMapMtx.Lock()
	defer client.configMapMtx.Unlock()
	client.configMap, err = client.clientset.CoreV1().ConfigMaps(client.namespace).Get(context.Background(), client.name, metav1.GetOptions{})
	if err == nil {
		if err := client.startController(); err != nil {
			return nil, err
		}
		return client, nil
	} else if !errors.IsNotFound(err) {
		return nil, err
	}

	// create a new config map
	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: client.name,
		},
		// We want non-empty .data and .binaryData; otherwise CAS will fail because it cannot find the parent key
		BinaryData: map[string][]byte{convertKeyToStore("_"): []byte("_")},
	}
	client.configMap, err = client.clientset.CoreV1().ConfigMaps(client.namespace).Create(context.Background(), configMap, metav1.CreateOptions{})
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

func convertKeyToStoreHash(in string) string {
	return hashPrefix + base64.RawURLEncoding.EncodeToString([]byte(in))
}

func convertKeyFromStore(in string) (string, error) {
	body, err := base64.RawURLEncoding.DecodeString(in)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func convertKeyFromStoreHash(in string) (string, error) {
	withoutPrefix := strings.TrimPrefix(in, hashPrefix)

	if withoutPrefix == in {
		return "", fmt.Errorf("key is not prefixed as a hash key")
	}

	return convertKeyFromStore(withoutPrefix)
}

// List returns a list of keys under the given prefix. Returned keys will
// include the prefix.
func (c *Client) List(_ context.Context, prefix string) ([]string, error) {
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
		if key == "_" { // the value we pre-populate the map with on creation
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
func (c *Client) Get(_ context.Context, key string) (interface{}, error) {
	c.configMapMtx.RLock()
	cm := c.configMap
	c.configMapMtx.RUnlock()

	if key == "_" { // the value we pre-populate the map with on creation
		return nil, nil
	}

	value, ok := cm.BinaryData[convertKeyToStore(key)]
	if !ok {
		return nil, nil
	}

	return c.codec.Decode(value)
}

// Delete a specific key. Deletions are best-effort and no error will
// be returned if the key does not exist.
func (c *Client) Delete(ctx context.Context, key string) error {
	c.configMapMtx.RLock()
	cm := c.configMap
	c.configMapMtx.RUnlock()

	_, ok := cm.BinaryData[convertKeyToStore(key)]
	if !ok {
		// Object is already deleted or never existed
		return nil
	}

	patch, err := prepareDeletePatch(key)
	if err != nil {
		return err
	}

	updatedCM, err := c.clientset.CoreV1().ConfigMaps(c.namespace).Patch(ctx, c.name, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}

	c.processCMUpdate(updatedCM)

	return nil
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
		patch        []byte
		encoded      []byte
		retry        = true

		cm *v1.ConfigMap
	)

	refreshIntermediate := func() error {
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
		return nil
	}

	for tries := 0; tries < maxCASRetries && retry; tries++ {
		c.metrics.casAttempts.Inc()

		err = refreshIntermediate()
		if err != nil {
			return err
		}

		intermediate, retry, err = f(intermediate)
		if err != nil {
			return err
		}

		if intermediate == nil {
			return nil
		}

		encoded, err = c.codec.Encode(intermediate)
		if err != nil {
			return err
		}

		oldEncodedHash := cm.BinaryData[convertKeyToStoreHash(key)]
		newHash := hash(encoded)

		patch, err = preparePatch(key, oldEncodedHash, encoded, newHash)
		if err != nil {
			return err
		}

		updatedCM, casErr := c.clientset.CoreV1().ConfigMaps(c.namespace).Patch(ctx, c.name, types.JSONPatchType, patch, metav1.PatchOptions{})

		if statusErr, ok := casErr.(*errors.StatusError); ok && statusErr.ErrStatus.Code == http.StatusUnprocessableEntity {
			err = casErr
			continue
		} else if casErr != nil {
			c.metrics.casFailures.Inc()
			return casErr
		}

		c.processCMUpdate(updatedCM)
		c.metrics.casSuccesses.Inc()
		return nil
	}

	c.metrics.casFailures.Inc()
	return fmt.Errorf("exceeded maximum CAS retries (%d)", maxCASRetries)
}

func hash(b []byte) []byte {
	hasher := sha1.New()
	_, err := hasher.Write(b)
	if err != nil {
		panic(err)
	}
	return hasher.Sum(nil)
}

func (c *Client) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	c.watcher.WatchKey(ctx, key, f)
}

func (c *Client) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	c.watcher.WatchPrefix(ctx, prefix, f)
}

func (c *Client) Stop(err error) error {
	close(c.stopCh)
	return c.watcher.Stop(err)
}
