package runtimeconfig

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
)

type TestLimits struct {
	Limit1 int `json:"limit1"`
	Limit2 int `json:"limit2"`
}

// WARNING: THIS GLOBAL VARIABLE COULD LEAD TO UNEXPECTED BEHAVIOUR WHEN RUNNING MULTIPLE DIFFERENT TESTS
var defaultTestLimits *TestLimits

type testOverrides struct {
	Overrides map[string]*TestLimits `yaml:"overrides"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *TestLimits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if defaultTestLimits != nil {
		*l = *defaultTestLimits
	}
	type plain TestLimits
	return unmarshal((*plain)(l))
}

func testLoadOverrides(r io.Reader) (interface{}, error) {
	var overrides = &testOverrides{}

	decoder := yaml.NewDecoder(r)
	decoder.SetStrict(true)
	if err := decoder.Decode(&overrides); err != nil {
		return nil, err
	}
	return overrides, nil
}

type value struct {
	Value int `yaml:"value"`
}

func valueLoader(r io.Reader) (i interface{}, err error) {
	v := value{Value: 0}
	buf, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(buf, &v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func writeValueToFile(t *testing.T, path string, v value) {
	t.Helper()
	buf, err := yaml.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path+".tmp", buf, 0777))
	// Atomically replace file with new file, so that manager cannot see unfinished modification.
	require.NoError(t, os.Rename(path+".tmp", path))
}

func newTestOverridesManagerConfig(t *testing.T, reloadPeriod time.Duration, loader func(reader io.Reader) (interface{}, error)) Config {
	// create empty file
	tempFile, err := os.CreateTemp("", "test-validation")
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	t.Cleanup(func() {
		_ = os.Remove(tempFile.Name())
	})

	// testing runtimeconfig Manager with overrides reload config set
	return Config{
		ReloadPeriod: reloadPeriod,
		LoadPath:     []string{tempFile.Name()},
		Loader:       loader,
	}
}

func generateRuntimeFiles(t *testing.T, overrideStrings []string) ([]*os.File, error) {
	var overrideFiles []*os.File

	t.Cleanup(func() {
		require.NoError(t, cleanupOverridesFiles(overrideFiles))
	})

	for count, override := range overrideStrings {
		pattern := fmt.Sprintf("overrides-file-%d", count)
		tempFile, err := os.CreateTemp("", pattern)
		if err != nil {
			return nil, err
		}
		_, err = tempFile.WriteString(override)
		if err != nil {
			return nil, err
		}
		overrideFiles = append(overrideFiles, tempFile)
	}

	return overrideFiles, nil
}

func generateLoadPath(overrideFiles []*os.File) []string {
	var fileNames []string
	for _, f := range overrideFiles {
		fileNames = append(fileNames, f.Name())
	}
	return fileNames
}

func cleanupOverridesFiles(overrideFiles []*os.File) error {
	for _, f := range overrideFiles {
		err := f.Close()
		if err != nil {
			return err
		}
		err = os.Remove(f.Name())
		if err != nil {
			return err
		}
	}
	return nil
}

func TestNewOverridesManager(t *testing.T) {
	tempFiles, err := generateRuntimeFiles(t,
		[]string{`overrides:
  user1:
    limit2: 150`})
	require.NoError(t, err)

	defaultTestLimits = &TestLimits{Limit1: 100}

	// testing runtimeconfig Manager with overrides reload config set
	overridesManagerConfig := Config{
		ReloadPeriod: time.Second,
		LoadPath:     generateLoadPath(tempFiles),
		Loader:       testLoadOverrides,
	}

	overridesManager, err := New(overridesManagerConfig, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
	conf := overridesManager.GetConfig().(*testOverrides)
	require.NotNil(t, conf)
	require.Equal(t, 150, conf.Overrides["user1"].Limit2)
}

func TestOverridesManagerMultipleFilesAppend(t *testing.T) {
	tempFiles, err := generateRuntimeFiles(t,
		[]string{`overrides:
  user1:
    limit1: 101`,
			`overrides:
  user1:
    limit2: 102`,
			`overrides:
  user2:
    limit1: 103`,
			`overrides:
  user2:
    limit2: 104`})
	require.NoError(t, err)

	// testing runtimeconfig Manager with overrides reload config set
	overridesManagerConfig := Config{
		ReloadPeriod: time.Second,
		LoadPath:     generateLoadPath(tempFiles),
		Loader:       testLoadOverrides,
	}

	overridesManager, err := New(overridesManagerConfig, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
	conf := overridesManager.GetConfig().(*testOverrides)
	require.Equal(t, 101, conf.Overrides["user1"].Limit1)
	require.Equal(t, 102, conf.Overrides["user1"].Limit2)
	require.Equal(t, 103, conf.Overrides["user2"].Limit1)
	require.Equal(t, 104, conf.Overrides["user2"].Limit2)
}

func TestOverridesManagerMultipleFilesWithOverrides(t *testing.T) {
	tempFiles, err := generateRuntimeFiles(t,
		[]string{
			`overrides:
  user1:
    limit1: 100`,
			`overrides:
  user1:
    limit1: 1234`})
	require.NoError(t, err)

	// testing runtimeconfig Manager with overrides reload config set
	overridesManagerConfig := Config{
		ReloadPeriod: time.Second,
		LoadPath:     flagext.StringSliceCSV(generateLoadPath(tempFiles)),
		Loader:       testLoadOverrides,
	}

	overridesManager, err := New(overridesManagerConfig, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
	conf := overridesManager.GetConfig().(*testOverrides)
	require.Equal(t, 1234, conf.Overrides["user1"].Limit1)
}

func TestOverridesManagerMultipleFilesWithEmptyFile(t *testing.T) {
	tempFiles, err := generateRuntimeFiles(t,
		[]string{`overrides:
  user1:
    limit1: 100`,
			``})
	require.NoError(t, err)

	// testing runtimeconfig Manager with overrides reload config set
	overridesManagerConfig := Config{
		ReloadPeriod: time.Second,
		LoadPath:     generateLoadPath(tempFiles),
		Loader:       testLoadOverrides,
	}

	overridesManager, err := New(overridesManagerConfig, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
	conf := overridesManager.GetConfig().(*testOverrides)
	require.Equal(t, 100, conf.Overrides["user1"].Limit1)
}

func TestManager_ListenerWithDefaultLimits(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test-validation")
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	defer func() {
		// Clean up
		require.NoError(t, os.Remove(tempFile.Name()))
	}()

	config := []byte(`overrides:
    user1:
        limit2: 150
`)
	err = os.WriteFile(tempFile.Name(), config, 0600)
	require.NoError(t, err)

	defaultTestLimits = &TestLimits{Limit1: 100}

	// testing NewRuntimeConfigManager with overrides reload config set
	overridesManagerConfig := Config{
		ReloadPeriod: time.Second,
		LoadPath:     []string{tempFile.Name()},
		Loader:       testLoadOverrides,
	}

	reg := prometheus.NewPedanticRegistry()

	overridesManager, err := New(overridesManagerConfig, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	// check if the metrics is set to the config map value before
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP runtime_config_hash Hash of the currently active runtime configuration, merged from all configured files.
					# TYPE runtime_config_hash gauge
					runtime_config_hash{sha256="%s"} 1
					# HELP runtime_config_last_reload_successful Whether the last runtime-config reload attempt was successful.
					# TYPE runtime_config_last_reload_successful gauge
					runtime_config_last_reload_successful 1
				`, fmt.Sprintf("%x", sha256.Sum256(config))))))

	// need to use buffer, otherwise loadConfig will throw away update
	ch := overridesManager.CreateListenerChannel(1)

	// rewrite file
	config = []byte(`overrides:
    user2:
        limit2: 200
`)
	err = os.WriteFile(tempFile.Name(), config, 0600)
	require.NoError(t, err)

	// Wait for reload.
	var newValue interface{}
	select {
	case newValue = <-ch:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("listener was not called")
	}

	to := newValue.(*testOverrides)
	require.Equal(t, 200, to.Overrides["user2"].Limit2) // new overrides
	require.Equal(t, 100, to.Overrides["user2"].Limit1) // from defaults

	// check if the metrics have been updated
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP runtime_config_hash Hash of the currently active runtime configuration, merged from all configured files.
					# TYPE runtime_config_hash gauge
					runtime_config_hash{sha256="%s"} 1
					# HELP runtime_config_last_reload_successful Whether the last runtime-config reload attempt was successful.
					# TYPE runtime_config_last_reload_successful gauge
					runtime_config_last_reload_successful 1
				`, fmt.Sprintf("%x", sha256.Sum256(config))))))

	// Cleaning up
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	// Make sure test limits were loaded.
	require.NotNil(t, overridesManager.GetConfig())
}

func TestManager_ListenerChannel(t *testing.T) {
	cfg := newTestOverridesManagerConfig(t, 500*time.Millisecond, valueLoader)

	writeValueToFile(t, cfg.LoadPath.String(), value{Value: 555})

	overridesManager, err := New(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)

	// need to use buffer, otherwise loadConfig will throw away update
	ch := overridesManager.CreateListenerChannel(1)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	select {
	case newValue := <-ch:
		require.Equal(t, value{Value: 555}, newValue)
	case <-time.After(5 * time.Second):
		t.Fatal("listener was not called")
	}

	writeValueToFile(t, cfg.LoadPath.String(), value{Value: 1111})

	select {
	case newValue := <-ch:
		require.Equal(t, value{Value: 1111}, newValue)
	case <-time.After(5 * time.Second):
		t.Fatal("listener was not called")
	}

	overridesManager.CloseListenerChannel(ch)
	select {
	case _, ok := <-ch:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("channel not closed")
	}
}

func TestManager_StopClosesListenerChannels(t *testing.T) {
	cfg := newTestOverridesManagerConfig(t, time.Second, valueLoader)

	overridesManager, err := New(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	ch := overridesManager.CreateListenerChannel(0)

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	select {
	case _, ok := <-ch:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("channel not closed")
	}
}

func TestManager_ShouldFastFailOnInvalidConfigAtStartup(t *testing.T) {
	// Create an invalid runtime config file.
	tempFile, err := os.CreateTemp("", "invalid-config")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.Remove(tempFile.Name()))
	})

	_, err = tempFile.Write([]byte("!invalid!"))
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	// Create the config manager and start it.
	cfg := Config{
		ReloadPeriod: time.Second,
		LoadPath:     []string{tempFile.Name()},
		Loader:       testLoadOverrides,
	}

	m, err := New(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.Error(t, services.StartAndAwaitRunning(context.Background(), m))
}

func TestManager_UnchangedFileDoesntTriggerReload(t *testing.T) {
	loadCounter := atomic.NewInt32(0)

	cfg := newTestOverridesManagerConfig(t, 100*time.Millisecond, func(reader io.Reader) (interface{}, error) {
		loadCounter.Inc()
		return valueLoader(reader)
	})

	overridesManager, err := New(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)

	ch := overridesManager.CreateListenerChannel(10) // must be big enough to hold all modifications.

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), overridesManager))

	test.Poll(t, time.Second, 1, func() interface{} {
		return int(loadCounter.Load())
	})

	// Let's make some modifications to the config
	const mods = 3
	const modDelay = 500 * time.Millisecond
	for i := 0; i < mods; i++ {
		writeValueToFile(t, cfg.LoadPath[0], value{Value: i})
		// wait before next rewrite, but also after last rewrite to give manager a chance to reload the file again
		time.Sleep(modDelay)
	}

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), overridesManager))

	assert.Equal(t, mods+1, int(loadCounter.Load())) // + 1 for initial load, before modifications
	assert.Equal(t, mods+1, len(ch))                 // Loaded values
}
