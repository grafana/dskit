package flagext

import (
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests in this file mutate os.Args and must NOT use t.Parallel().

func TestParseFlagsAndArgumentsWithEnv(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var host string
	var port int
	fs.StringVar(&host, "server.host", "localhost", "host")
	fs.IntVar(&port, "server.port", 80, "port")

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"cmd", "-server.host", "clihost"}

	t.Setenv("APP_SERVER_PORT", "9090")

	args, err := ParseFlagsAndArgumentsWithEnv(fs, "APP")
	require.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "clihost", host, "CLI flag should be used")
	assert.Equal(t, 9090, port, "env var should set unset flag")
}

func TestParseFlagsWithoutArgumentsWithEnv(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var host string
	var port int
	fs.StringVar(&host, "server.host", "localhost", "host")
	fs.IntVar(&port, "server.port", 80, "port")

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"cmd", "-server.host", "clihost"}

	t.Setenv("APP_SERVER_PORT", "9090")

	err := ParseFlagsWithoutArgumentsWithEnv(fs, "APP")
	require.NoError(t, err)
	assert.Equal(t, "clihost", host, "CLI flag should be used")
	assert.Equal(t, 9090, port, "env var should set unset flag")
}

func TestParseFlagsWithoutArgumentsWithEnv_RejectsArguments(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.String("flag", "default", "a flag")

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"cmd", "unexpected-argument"}

	err := ParseFlagsWithoutArgumentsWithEnv(fs, "APP")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected-argument")
}

func TestParseFlagsAndArgumentsWithEnv_CLIPrecedence(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	fs.IntVar(&port, "port", 80, "port")

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"cmd", "-port", "3000"}

	t.Setenv("APP_PORT", "9090")

	_, err := ParseFlagsAndArgumentsWithEnv(fs, "APP")
	require.NoError(t, err)
	assert.Equal(t, 3000, port, "CLI flag should take precedence over env var")
}

func TestParseFlagsAndArgumentsWithEnv_EnvError(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Int("port", 80, "port")

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"cmd"}

	t.Setenv("APP_PORT", "not-a-number")

	_, err := ParseFlagsAndArgumentsWithEnv(fs, "APP")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "port")
}

func TestParseFlagsAndArgumentsWithEnv_ReturnsPositionalArgs(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var port int
	fs.IntVar(&port, "port", 80, "port")

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })
	os.Args = []string{"cmd", "-port", "3000", "arg1", "arg2"}

	args, err := ParseFlagsAndArgumentsWithEnv(fs, "APP")
	require.NoError(t, err)
	assert.Equal(t, []string{"arg1", "arg2"}, args)
	assert.Equal(t, 3000, port)
}
