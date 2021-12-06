package watch

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
)

func TestWatcher(t *testing.T) {
	t.Run("Notify notifies watchers that are watching a key", func(t *testing.T) {
		w := newTestWatcher()
		const key = "/key"
		notifyVal := rand.Int()
		notified := make(chan struct{})

		go w.WatchKey(context.Background(), key, func(val interface{}) bool {
			assert.Equal(t, notifyVal, val)
			close(notified)
			return false
		})

		// Wait until the watcher has been registered
		require.NoError(t, wait.Poll(time.Millisecond*10, time.Second, func() (bool, error) {
			return w.keyWatchersCount() > 0, nil
		}))

		w.Notify(key, notifyVal)

		select {
		case <-time.NewTimer(time.Second).C:
			assert.FailNow(t, "notifier didn't get called within a second")
		case <-notified:
		}
	})

	t.Run("Notify notifies watchers that are watching a prefix", func(t *testing.T) {
		w := newTestWatcher()

		const prefix = "/prefix"
		const key = prefix + "/key"
		notifyVal := rand.Int()
		notified := make(chan struct{})

		go w.WatchPrefix(context.Background(), prefix, func(k string, val interface{}) bool {
			assert.Equal(t, notifyVal, val)
			assert.Equal(t, key, k)
			close(notified)
			return false
		})

		// Wait until the watcher has been registered
		require.NoError(t, wait.Poll(time.Millisecond*10, time.Second, func() (bool, error) {
			return w.prefixWatchersCount() > 0, nil
		}))

		w.Notify(key, notifyVal)

		select {
		case <-time.NewTimer(time.Second).C:
			assert.FailNow(t, "notifier didn't get called within a second")
		case <-notified:
		}
	})
}

func newTestWatcher() *Watcher {
	var logger log.Logger
	if testing.Verbose() {
		logger = log.NewLogfmtLogger(os.Stdout)
	} else {
		logger = log.NewNopLogger()
	}

	return NewWatcher(logger)
}
