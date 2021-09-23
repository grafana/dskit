package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYoloBuf(t *testing.T) {
	s := yoloBuf("hello world")

	require.Equal(t, []byte("hello world"), s)
}
