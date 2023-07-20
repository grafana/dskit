package loser_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"

	"github.com/grafana/dskit/loser"
)

func checkTreeEqual[E constraints.Ordered](t *testing.T, tree *loser.Tree[E], expected []E, msg ...interface{}) {
	t.Helper()
	actual := []E{}

	for tree.Next() {
		actual = append(actual, tree.Winner())
	}

	require.Equal(t, expected, actual, msg...)
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
	{
		name: "two lists, largest value equal to one less than maximum",
		args: [][]uint64{{1, 3}, {2, math.MaxUint64 - 1}},
		want: []uint64{1, 2, 3, math.MaxUint64 - 1},
	},
	{
		name: "two lists, largest value in first list equal to maximum",
		args: [][]uint64{{1, math.MaxUint64}, {2, 3}},
		want: []uint64{1, 2, 3, math.MaxUint64},
	},
	{
		name: "two lists, first straddles second and has largest value equal to maximum",
		args: [][]uint64{{1, 3, math.MaxUint64}, {2}},
		want: []uint64{1, 2, 3, math.MaxUint64},
	},
	{
		name: "two lists, largest value in second list equal to maximum",
		args: [][]uint64{{1, 3}, {2, math.MaxUint64}},
		want: []uint64{1, 2, 3, math.MaxUint64},
	},
	{
		name: "two lists, second straddles first and has largest value equal to maximum",
		args: [][]uint64{{2}, {1, 3, math.MaxUint64}},
		want: []uint64{1, 2, 3, math.MaxUint64},
	},
	{
		name: "two lists, largest value in both lists equal to maximum",
		args: [][]uint64{{1, math.MaxUint64}, {2, math.MaxUint64}},
		want: []uint64{1, 2, math.MaxUint64, math.MaxUint64},
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

func FuzzMerge(f *testing.F) {
	f.Fuzz(func(t *testing.T, seed int64) {
		r := rand.New(rand.NewSource(seed))
		listCount := r.Intn(9) + 1
		lists := make([][]uint64, listCount)
		allElements := []uint64{}

		for listIdx := 0; listIdx < listCount; listIdx++ {
			elementCount := r.Intn(5)
			list := make([]uint64, elementCount)

			for elementIdx := 0; elementIdx < elementCount; elementIdx++ {
				list[elementIdx] = r.Uint64()
			}

			slices.Sort(list)
			allElements = append(allElements, list...)
			lists[listIdx] = list
		}

		lt := loser.New(lists, math.MaxUint64)
		slices.Sort(allElements)
		checkTreeEqual(t, lt, allElements, fmt.Sprintf("merging %v", lists))
	})
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
