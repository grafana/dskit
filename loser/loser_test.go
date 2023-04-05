package loser_test

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
	"math"
	"testing"

	"github.com/grafana/dskit/loser"
)

func checkTreeEqual[E constraints.Ordered](t *testing.T, tree *loser.Tree[E], expected []E) {
	t.Helper()
	actual := []E{}

	for tree.Next() {
		actual = append(actual, tree.Winner())
	}

	require.Equal(t, expected, actual)
}

var testCases = []struct {
	name string
	args [][]uint64
	want []uint64
}{
	{
		name: "empty input",
		want: []uint64{},
	},
	{
		name: "one list",
		args: [][]uint64{{1, 2, 3, 4}},
		want: []uint64{1, 2, 3, 4},
	},
	{
		name: "two lists",
		args: [][]uint64{{3, 4, 5}, {1, 2}},
		want: []uint64{1, 2, 3, 4, 5},
	},
	{
		name: "two lists, first empty",
		args: [][]uint64{{}, {1, 2}},
		want: []uint64{1, 2},
	},
	{
		name: "two lists, second empty",
		args: [][]uint64{{1, 2}, {}},
		want: []uint64{1, 2},
	},
	{
		name: "two lists b",
		args: [][]uint64{{1, 2}, {3, 4, 5}},
		want: []uint64{1, 2, 3, 4, 5},
	},
	{
		name: "two lists c",
		args: [][]uint64{{1, 3}, {2, 4, 5}},
		want: []uint64{1, 2, 3, 4, 5},
	},
	{
		name: "three lists",
		args: [][]uint64{{1, 3}, {2, 4}, {5}},
		want: []uint64{1, 2, 3, 4, 5},
	},
}

func TestMerge(t *testing.T) {
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			lt := loser.New(tt.args, math.MaxUint64)
			checkTreeEqual(t, lt, tt.want)
		})
	}
}

func TestPush(t *testing.T) {
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			lt := loser.New[uint64](nil, math.MaxUint64)
			for _, s := range tt.args {
				lt.Push(s)
			}
			checkTreeEqual(t, lt, tt.want)
		})
	}
}
