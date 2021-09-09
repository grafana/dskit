package yolo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYoloBuf(t *testing.T) {
	s := Buf("hello world")

	require.Equal(t, []byte("hello world"), s)
}
