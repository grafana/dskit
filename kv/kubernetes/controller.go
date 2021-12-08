package kubernetes

import (
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

func (c *Client) startController() error {
	// create the pod watcher
	configMapWatcher := cache.NewFilteredListWatchFromClient(c.clientset.CoreV1().RESTClient(), "configMaps", c.namespace, func(options *metav1.ListOptions) {
		options.FieldSelector = "metadata.name=" + c.name
	})

	// create the workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	indexer, informer := cache.NewIndexerInformer(configMapWatcher, &v1.ConfigMap{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer uses a delta queue, therefore for deletes we have to use this
			// key function.
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	}, cache.Indexers{})

	c.queue = queue
	c.indexer = indexer
	c.informer = informer

	go c.runController()

	return nil
}

func (c *Client) runController() {
	defer runtime.HandleCrash()

	// Let the workers stop when we are done
	defer c.queue.ShutDown()
	c.logger.Log("msg", "starting configmap controller")

	go c.informer.Run(c.stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(c.stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	go wait.Until(func() {
		for c.process() {
		}
	}, time.Second, c.stopCh)

	<-c.stopCh
	c.logger.Log("msg", "stopping configmap controller")
}

func (c *Client) process() bool {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// Tell the queue that we are done with processing this key. This unblocks the key for other workers
	// This allows safe parallel processing because two pods with the same key are never processed in
	// parallel.
	defer c.queue.Done(key)

	obj, exists, err := c.indexer.GetByKey(key.(string))
	if err != nil {
		c.logger.Log("Fetching object with key %s from store failed with %v", key.(string), err)
		return false
	}

	if !exists {
		level.Warn(c.logger).Log("msg", "configMap does not exist anymore", "name", key)
	} else {
		c.processCMUpdate(obj.(*v1.ConfigMap))
	}
	return true
}

func (c *Client) processCMUpdate(newMap *v1.ConfigMap) {
	c.configMapMtx.Lock()
	oldMap := c.configMap
	c.configMap = newMap
	c.configMapMtx.Unlock()

	for key, keyHash := range newMap.BinaryData {
		decodedKey, err := convertKeyFromStoreHash(key)
		if err != nil {
			continue // skip non-hash keys
		}
		if string(oldMap.BinaryData[key]) == string(keyHash) {
			continue
		}

		keyContents := newMap.BinaryData[convertKeyToStore(decodedKey)]

		decoded, err := c.codec.Decode(keyContents)
		if err != nil {
			level.Warn(c.logger).Log("msg", "couldn't deserialize key contents", "error", err, "key", decodedKey)
			continue
		}
		c.watcher.Notify(decodedKey, decoded)
	}
}
