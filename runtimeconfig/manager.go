package runtimeconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/dskit/services"
)

// Loader loads the configuration from file.
type Loader func(r io.Reader) (interface{}, error)

const (
	PeriodicReload = "periodic-reload"
	Notify         = "notify"
)

// Config holds the config for an Manager instance.
// It holds config related to loading per-tenant config.
type Config struct {
	ReloadPeriod time.Duration `yaml:"period" category:"advanced"`
	// LoadPath contains the path to the runtime config file, requires an
	// non-empty value
	LoadPath string `yaml:"file"`
	Loader   Loader `yaml:"-"`
	// optional
	ChangeDetector string `yaml:"change_detector" category:"experimental"`
}

// RegisterFlags registers flags.
func (mc *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&mc.LoadPath, "runtime-config.file", "", "File with the configuration that can be updated in runtime.")
	f.DurationVar(&mc.ReloadPeriod, "runtime-config.reload-period", 10*time.Second, "How often to check runtime config file.")
	f.StringVar(&mc.ChangeDetector, "runtime-config.change-detector", PeriodicReload,
		"How to detect changes in the runtime configuration. Supported values are: periodic-reload and notify. The notify option may not work in every case e.g. NFS")
}

// Manager periodically reloads the configuration from a file, and keeps this
// configuration available for clients.
type Manager struct {
	services.Service

	cfg    Config
	logger log.Logger

	listenersMtx sync.Mutex
	listeners    []chan interface{}

	configMtx sync.RWMutex
	config    interface{}

	configLoadSuccess       prometheus.Gauge
	configHash              *prometheus.GaugeVec
	configNotificationWakes prometheus.Counter
}

// New creates an instance of Manager and starts reload config loop based on config
func New(cfg Config, registerer prometheus.Registerer, logger log.Logger) (*Manager, error) {
	if cfg.LoadPath == "" {
		return nil, errors.New("LoadPath is empty")
	}

	mgr := Manager{
		cfg: cfg,
		configLoadSuccess: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "runtime_config_last_reload_successful",
			Help: "Whether the last runtime-config reload attempt was successful.",
		}),
		configHash: promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
			Name: "runtime_config_hash",
			Help: "Hash of the currently active runtime config file.",
		}, []string{"sha256"}),
		configNotificationWakes: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "runtime_config_notification_wakes",
			Help: "The number of times file notifications caused a wake.",
		}),

		logger: logger,
	}

	mgr.Service = services.NewBasicService(mgr.starting, mgr.detectChanges, mgr.stopping)
	return &mgr, nil
}

func (om *Manager) starting(_ context.Context) error {
	if om.cfg.LoadPath == "" {
		return nil
	}

	return errors.Wrap(om.loadConfig(), "failed to load runtime config")
}

// CreateListenerChannel creates new channel that can be used to receive new config values.
// If there is no receiver waiting for value when config manager tries to send the update,
// or channel buffer is full, update is discarded.
//
// When config manager is stopped, it closes all channels to notify receivers that they will
// not receive any more updates.
func (om *Manager) CreateListenerChannel(buffer int) <-chan interface{} {
	ch := make(chan interface{}, buffer)

	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	om.listeners = append(om.listeners, ch)
	return ch
}

// CloseListenerChannel removes given channel from list of channels to send notifications to and closes channel.
func (om *Manager) CloseListenerChannel(listener <-chan interface{}) {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for ix, ch := range om.listeners {
		if ch == listener {
			om.listeners = append(om.listeners[:ix], om.listeners[ix+1:]...)
			close(ch)
			break
		}
	}
}

func (om *Manager) detectChanges(ctx context.Context) error {
	if om.cfg.LoadPath == "" {
		level.Info(om.logger).Log("msg", "runtime config disabled: file not specified")
		<-ctx.Done()
		return nil
	}

	if om.cfg.ChangeDetector == "" || om.cfg.ChangeDetector == PeriodicReload {
		om.periodicReload(ctx)
	} else if om.cfg.ChangeDetector == Notify {
		err := om.watchFileNotifications(ctx)
		if err != nil {
			level.Error(om.logger).Log("msg", "failed to initialize runtime configuration notify loop", "err", err)
			return err
		}
	} else {
		return errors.Errorf("Unsupported runtime configuration change detector: %s", om.cfg.ChangeDetector)
	}

	return nil
}

// watchForNotifications utilizes file notifications to improve responsiveness of runtime configurtion loading
// Overall this tricky to get right, see the inotify man page and https://github.com/fsnotify/fsnotify/issues/372 for a list of issues
func (om *Manager) watchFileNotifications(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	// If the file is a symlink follow it, otherwise modifications could be missed
	symlinkFp, err := filepath.EvalSymlinks(om.cfg.LoadPath)
	if err != nil {
		return err
	}
	fp, err := filepath.Abs(symlinkFp)
	if err != nil {
		return err
	}
	// Watch the directory rather than the file directly to handle the file's remove/add, since inodes are watched not names
	dir, _ := filepath.Split(fp)
	if dir == "" {
		return errors.New("Could not determine path to watch for notifications from configured load path")
	}

	err = watcher.Add(dir)
	if err != nil {
		return err
	}
	om.logger.Log("Watching %s", dir)

	done := make(chan bool)
	go om.notifyLoop(ctx, watcher, fp, done)
	<-done
	<-ctx.Done()

	return nil
}

func (om *Manager) notifyLoop(ctx context.Context, watcher *fsnotify.Watcher, filePath string, done chan<- bool) {
	// Still have background reads as a failsafe. This value can be configured higher if notifications are providing responsiveness.
	ticker := time.NewTicker(om.cfg.ReloadPeriod)

	defer func() {
		ticker.Stop()
		done <- true
	}()

	opMask := fsnotify.Write | fsnotify.Create

	for {
		select {
		case <-ticker.C:
			om.attemptLoadConfig()
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			om.configNotificationWakes.Inc()

			if filePath == event.Name && (event.Op&opMask != 0) {
			drain:
				for {
					select {
					case <-watcher.Events:
						// Drain any remaining events before reading; they are unnecessary
					default:
						break drain
					}
				}
				om.attemptLoadConfig()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			level.Error(om.logger).Log("msg", "error while watching runtime config", "err", err)
		case <-ctx.Done():
			return
		}
	}
}

// periodicReload loads the configuration every reload period until stopPredicate returns true or the context is closed
func (om *Manager) periodicReload(ctx context.Context) {
	ticker := time.NewTicker(om.cfg.ReloadPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			om.attemptLoadConfig()
		case <-ctx.Done():
			return
		}
	}
}

func (om *Manager) attemptLoadConfig() {
	err := om.loadConfig()
	if err != nil {
		// Log but don't stop on error - we don't want to halt all ingesters because of a typo
		level.Error(om.logger).Log("msg", "failed to load config", "err", err)
	}
}

// loadConfig loads configuration using the loader function, and if successful,
// stores it as current configuration and notifies listeners.
func (om *Manager) loadConfig() error {
	buf, err := os.ReadFile(om.cfg.LoadPath)
	if err != nil {
		om.configLoadSuccess.Set(0)
		return errors.Wrap(err, "read file")
	}
	hash := sha256.Sum256(buf)

	cfg, err := om.cfg.Loader(bytes.NewReader(buf))
	if err != nil {
		om.configLoadSuccess.Set(0)
		return errors.Wrap(err, "load file")
	}
	om.configLoadSuccess.Set(1)

	om.setConfig(cfg)
	om.callListeners(cfg)

	// expose hash of runtime config
	om.configHash.Reset()
	om.configHash.WithLabelValues(fmt.Sprintf("%x", hash[:])).Set(1)

	return nil
}

func (om *Manager) setConfig(config interface{}) {
	om.configMtx.Lock()
	defer om.configMtx.Unlock()
	om.config = config
}

func (om *Manager) callListeners(newValue interface{}) {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for _, ch := range om.listeners {
		select {
		case ch <- newValue:
			// ok
		default:
			// nobody is listening or buffer full.
		}
	}
}

// Stop stops the Manager
func (om *Manager) stopping(_ error) error {
	om.listenersMtx.Lock()
	defer om.listenersMtx.Unlock()

	for _, ch := range om.listeners {
		close(ch)
	}
	om.listeners = nil
	return nil
}

// GetConfig returns last loaded config value, possibly nil.
func (om *Manager) GetConfig() interface{} {
	om.configMtx.RLock()
	defer om.configMtx.RUnlock()

	return om.config
}
