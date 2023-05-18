package flagext

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func Test_Bytes_String(t *testing.T) {
	for _, tcase := range []struct {
		value uint64
		str   string
	}{
		{
			value: 1024 * 1024 * 1024,
			str:   "1GiB",
		},
		{
			value: 1024 * 1024,
			str:   "1MiB",
		},
		{
			value: 1024,
			str:   "1KiB",
		},
	} {
		b := Bytes(tcase.value)
		require.Equal(t, tcase.str, b.String())
	}
}

func Test_Bytes_Set(t *testing.T) {
	for _, tcase := range []struct {
		value uint64
		str   string
	}{
		{
			value: 1024 * 1024 * 1024,
			str:   "1GiB",
		},
		{
			value: 1024 * 1024 * 1024,
			str:   "1GB",
		},
		{
			value: 1024 * 1024,
			str:   "1MiB",
		},
		{
			value: 1024 * 1024,
			str:   "1MB",
		},
		{
			value: 1024,
			str:   "1KiB",
		},
		{
			value: 1024,
			str:   "1KB",
		},
	} {
		var b Bytes
		require.NoError(t, b.Set(tcase.str))
		require.Equal(t, b, Bytes(tcase.value))
	}
}
func Test_Bytes_MarshalYAML(t *testing.T) {
	for _, tcase := range []struct {
		value uint64
		str   string
	}{
		{
			value: 1024 * 1024 * 1024,
			str:   "1GiB\n",
		},
		{
			value: 1024 * 1024,
			str:   "1MiB\n",
		},
		{
			value: 1024,
			str:   "1KiB\n",
		},
	} {
		b := Bytes(tcase.value)
		y, err := yaml.Marshal(&b)
		require.NoError(t, err)
		require.Equal(t, []byte(tcase.str), y)
	}
}

func Test_Bytes_UnmarshalYAML(t *testing.T) {
	for _, tcase := range []struct {
		value uint64
		str   string
	}{
		{
			value: 1024 * 1024 * 1024,
			str:   "1GiB\n",
		},
		{
			value: 1024 * 1024 * 1024,
			str:   "1GB\n",
		},
		{
			value: 1024 * 1024,
			str:   "1MiB\n",
		},
		{
			value: 1024 * 1024,
			str:   "1MB\n",
		},
		{
			value: 1024,
			str:   "1KiB\n",
		},
		{
			value: 1024,
			str:   "1KB\n",
		},
	} {
		var b Bytes
		require.NoError(t, yaml.Unmarshal([]byte(tcase.str), &b))
		require.Equal(t, Bytes(tcase.value), b)
	}
}
