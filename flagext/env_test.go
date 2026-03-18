package flagext

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlagToEnvVar(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		flagName string
		expected string
	}{
		{
			name:     "basic flag with prefix",
			prefix:   "MYAPP",
			flagName: "server.http-listen-port",
			expected: "MYAPP_SERVER_HTTP_LISTEN_PORT",
		},
		{
			name:     "empty prefix",
			prefix:   "",
			flagName: "server.http-listen-port",
			expected: "SERVER_HTTP_LISTEN_PORT",
		},
		{
			name:     "simple flag name",
			prefix:   "APP",
			flagName: "verbose",
			expected: "APP_VERBOSE",
		},
		{
			name:     "multiple dots and dashes",
			prefix:   "MIMIR",
			flagName: "store.engine-config.max-retries",
			expected: "MIMIR_STORE_ENGINE_CONFIG_MAX_RETRIES",
		},
		{
			name:     "already uppercase flag name",
			prefix:   "APP",
			flagName: "TLS",
			expected: "APP_TLS",
		},
		{
			name:     "flag name with underscores",
			prefix:   "APP",
			flagName: "my_flag",
			expected: "APP_MY_FLAG",
		},
		{
			name:     "both empty",
			prefix:   "",
			flagName: "",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := FlagToEnvVar(tc.prefix, tc.flagName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func mapLookup(m map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func TestSetFlagsFromEnv_Basic(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	fs.IntVar(&port, "server.http-listen-port", 80, "HTTP listen port")

	require.NoError(t, fs.Parse([]string{}))

	env := map[string]string{
		"MYAPP_SERVER_HTTP_LISTEN_PORT": "9090",
	}
	err := SetFlagsFromEnvWithLookup(fs, "MYAPP", mapLookup(env))
	require.NoError(t, err)
	assert.Equal(t, 9090, port)
}

func TestSetFlagsFromEnv_CLIPrecedence(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	fs.IntVar(&port, "server.http-listen-port", 80, "HTTP listen port")

	require.NoError(t, fs.Parse([]string{"-server.http-listen-port", "3000"}))

	env := map[string]string{
		"MYAPP_SERVER_HTTP_LISTEN_PORT": "9090",
	}
	err := SetFlagsFromEnvWithLookup(fs, "MYAPP", mapLookup(env))
	require.NoError(t, err)
	assert.Equal(t, 3000, port, "CLI flag should take precedence over env var")
}

func TestSetFlagsFromEnv_DefaultPrecedence(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var host string
	fs.StringVar(&host, "server.host", "localhost", "Server host")

	require.NoError(t, fs.Parse([]string{}))

	env := map[string]string{
		"APP_SERVER_HOST": "0.0.0.0",
	}
	err := SetFlagsFromEnvWithLookup(fs, "APP", mapLookup(env))
	require.NoError(t, err)
	assert.Equal(t, "0.0.0.0", host, "env var should override default")
}

func TestSetFlagsFromEnv_NoEnvVar(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	fs.IntVar(&port, "server.http-listen-port", 80, "HTTP listen port")

	require.NoError(t, fs.Parse([]string{}))

	err := SetFlagsFromEnvWithLookup(fs, "MYAPP", mapLookup(nil))
	require.NoError(t, err)
	assert.Equal(t, 80, port, "default should be preserved when no env var is set")
}

func TestSetFlagsFromEnv_InvalidValue(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	fs.IntVar(&port, "server.http-listen-port", 80, "HTTP listen port")

	require.NoError(t, fs.Parse([]string{}))

	env := map[string]string{
		"MYAPP_SERVER_HTTP_LISTEN_PORT": "not-a-number",
	}
	err := SetFlagsFromEnvWithLookup(fs, "MYAPP", mapLookup(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server.http-listen-port")
	assert.Contains(t, err.Error(), "MYAPP_SERVER_HTTP_LISTEN_PORT")
}

func TestSetFlagsFromEnv_BoolFlag(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected bool
	}{
		{"true", "true", true},
		{"false", "false", false},
		{"1", "1", true},
		{"0", "0", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			var verbose bool
			fs.BoolVar(&verbose, "verbose", false, "verbose mode")

			require.NoError(t, fs.Parse([]string{}))

			env := map[string]string{
				"APP_VERBOSE": tc.envValue,
			}
			err := SetFlagsFromEnvWithLookup(fs, "APP", mapLookup(env))
			require.NoError(t, err)
			assert.Equal(t, tc.expected, verbose)
		})
	}
}

func TestSetFlagsFromEnv_MultipleFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var host string
	var port int
	var verbose bool
	fs.StringVar(&host, "server.host", "localhost", "host")
	fs.IntVar(&port, "server.port", 80, "port")
	fs.BoolVar(&verbose, "verbose", false, "verbose")

	require.NoError(t, fs.Parse([]string{}))

	env := map[string]string{
		"APP_SERVER_HOST": "0.0.0.0",
		"APP_SERVER_PORT": "9090",
		"APP_VERBOSE":     "true",
	}
	err := SetFlagsFromEnvWithLookup(fs, "APP", mapLookup(env))
	require.NoError(t, err)
	assert.Equal(t, "0.0.0.0", host)
	assert.Equal(t, 9090, port)
	assert.True(t, verbose)
}

func TestSetFlagsFromEnv_EmptyEnvValue(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var host string
	fs.StringVar(&host, "server.host", "localhost", "host")

	require.NoError(t, fs.Parse([]string{}))

	env := map[string]string{
		"APP_SERVER_HOST": "",
	}
	err := SetFlagsFromEnvWithLookup(fs, "APP", mapLookup(env))
	require.NoError(t, err)
	assert.Equal(t, "", host, "empty env var should set flag to empty string")
}

func TestSetFlagsFromEnv_CustomFlagTypes(t *testing.T) {
	t.Run("StringSliceCSV", func(t *testing.T) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		var csv StringSliceCSV
		fs.Var(&csv, "server.allowed-origins", "allowed origins")

		require.NoError(t, fs.Parse([]string{}))

		env := map[string]string{
			"APP_SERVER_ALLOWED_ORIGINS": "http://localhost,http://example.com",
		}
		err := SetFlagsFromEnvWithLookup(fs, "APP", mapLookup(env))
		require.NoError(t, err)
		assert.Equal(t, StringSliceCSV{"http://localhost", "http://example.com"}, csv)
	})

	t.Run("Secret", func(t *testing.T) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		var secret Secret
		fs.Var(&secret, "auth.token", "auth token")

		require.NoError(t, fs.Parse([]string{}))

		env := map[string]string{
			"APP_AUTH_TOKEN": "super-secret",
		}
		err := SetFlagsFromEnvWithLookup(fs, "APP", mapLookup(env))
		require.NoError(t, err)
		assert.Equal(t, SecretWithValue("super-secret"), secret)
	})
}

func TestSetFlagsFromEnv_EmptyPrefix(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	fs.IntVar(&port, "server.port", 80, "port")

	require.NoError(t, fs.Parse([]string{}))

	env := map[string]string{
		"SERVER_PORT": "9090",
	}
	err := SetFlagsFromEnvWithLookup(fs, "", mapLookup(env))
	require.NoError(t, err)
	assert.Equal(t, 9090, port)
}

func TestSetFlagsFromEnv_MixedCLIAndEnv(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var host string
	var port int
	fs.StringVar(&host, "server.host", "localhost", "host")
	fs.IntVar(&port, "server.port", 80, "port")

	// Only set host via CLI
	require.NoError(t, fs.Parse([]string{"-server.host", "myhost"}))

	env := map[string]string{
		"APP_SERVER_HOST": "envhost",
		"APP_SERVER_PORT": "9090",
	}
	err := SetFlagsFromEnvWithLookup(fs, "APP", mapLookup(env))
	require.NoError(t, err)
	assert.Equal(t, "myhost", host, "CLI-set flag should not be overridden")
	assert.Equal(t, 9090, port, "unset flag should get env var value")
}

func TestSetFlagsFromEnv_MultipleErrors(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	var timeout float64
	fs.IntVar(&port, "port", 80, "port")
	fs.Float64Var(&timeout, "timeout", 1.0, "timeout")

	require.NoError(t, fs.Parse([]string{}))

	env := map[string]string{
		"APP_PORT":    "not-int",
		"APP_TIMEOUT": "not-float",
	}
	err := SetFlagsFromEnvWithLookup(fs, "APP", mapLookup(env))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "port")
	assert.Contains(t, err.Error(), "timeout")
}

func TestSetFlagsFromEnv_WithRealEnv(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	fs.IntVar(&port, "server.port", 80, "port")

	require.NoError(t, fs.Parse([]string{}))

	t.Setenv("APP_SERVER_PORT", "9090")

	err := SetFlagsFromEnv(fs, "APP")
	require.NoError(t, err)
	assert.Equal(t, 9090, port)
}
