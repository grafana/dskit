package watch

import (
	"context"
	"strings"
	"sync"

	"github.com/go-kit/log"

	"github.com/go-kit/log/level"
)

// TODO dimitarvdimitrov this is code copied form memberlist; make sure to refactor memberlist to use this implementation before merging

type Watcher struct {
	logger log.Logger

	// closed on shutdown
	shutdown chan struct{}

	// Key watchers
	watchersMu     sync.Mutex
	watchers       map[string][]chan update
	prefixWatchers map[string][]chan update
}

type update struct {
	key   string
	value interface{}
}

func NewWatcher(l log.Logger) *Watcher {
	return &Watcher{
		logger:         l,
		shutdown:       make(chan struct{}),
		watchers:       make(map[string][]chan update),
		prefixWatchers: make(map[string][]chan update),
	}
}

// WatchKey watches for value changes for given key. When value changes, 'f' function is called with the
// latest value. Notifications that arrive while 'f' is running are coalesced into one subsequent 'f' call.
//
// Watching ends when 'f' returns false, context is done, or this client is shut down.
func (w *Watcher) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	// keep one extra notification, to avoid missing notification if we're busy running the function
	watchChan := make(chan update, 1)

	// register watcher
	w.watchersMu.Lock()
	w.watchers[key] = append(w.watchers[key], watchChan)
	w.watchersMu.Unlock()

	defer func() {
		// unregister watcher on exit
		w.watchersMu.Lock()
		defer w.watchersMu.Unlock()

		removeWatcherChannel(key, watchChan, w.watchers)
	}()

	for {
		select {
		case newValue := <-watchChan:
			// value changed
			if !f(newValue.value) {
				return
			}

		case <-w.shutdown:
			// stop watching on shutdown
			return

		case <-ctx.Done():
			return
		}
	}
}

// WatchPrefix watches for any change of values stored under keys with given prefix. When change occurs,
// function 'f' is called with key and current value.
// Each change of the key results in one notification. If there are too many pending notifications ('f' is slow),
// some notifications may be lost.
//
// Watching ends when 'f' returns false, context is done, or this client is shut down.
func (w *Watcher) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	// we use bigger buffer here, since keys are interesting and we don't want to lose them.
	watchChan := make(chan update, 16)

	// register watcher
	w.watchersMu.Lock()
	w.prefixWatchers[prefix] = append(w.prefixWatchers[prefix], watchChan)
	w.watchersMu.Unlock()

	defer func() {
		// unregister watcher on exit
		w.watchersMu.Lock()
		defer w.watchersMu.Unlock()

		removeWatcherChannel(prefix, watchChan, w.prefixWatchers)
	}()

	for {
		select {
		case newValue := <-watchChan:
			if !f(newValue.key, newValue.value) {
				return
			}

		case <-w.shutdown:
			// stop watching on shutdown
			return

		case <-ctx.Done():
			return
		}
	}
}

func removeWatcherChannel(k string, w chan update, watchers map[string][]chan update) {
	ws := watchers[k]
	for ix, kw := range ws {
		if kw == w {
			ws = append(ws[:ix], ws[ix+1:]...)
			break
		}
	}

	if len(ws) > 0 {
		watchers[k] = ws
	} else {
		delete(watchers, k)
	}
}

func (w *Watcher) Notify(key string, value interface{}) {
	w.watchersMu.Lock()
	defer w.watchersMu.Unlock()

	watchKey := update{
		key:   key,
		value: value,
	}

	for _, kw := range w.watchers[key] {
		select {
		case kw <- watchKey:
			// notification sent.
		default:
			// cannot send notification to this watcher at the moment
			// but since this is a buffered channel, it means that
			// there is already a pending notification anyway
		}
	}

	for p, ws := range w.prefixWatchers {
		if strings.HasPrefix(key, p) {
			for _, pw := range ws {
				select {
				case pw <- watchKey:
					// notification sent.
				default:
					level.Warn(w.logger).Log("msg", "failed to send notification to prefix watcher", "prefix", p)
				}
			}
		}
	}
}

func (w *Watcher) prefixWatchersCount() int {
	w.watchersMu.Lock()
	defer w.watchersMu.Unlock()
	return len(w.prefixWatchers)
}

func (w *Watcher) keyWatchersCount() int {
	w.watchersMu.Lock()
	defer w.watchersMu.Unlock()
	return len(w.watchers)
}

func (w *Watcher) Stop(_ error) error {
	close(w.shutdown)
	return nil
}
