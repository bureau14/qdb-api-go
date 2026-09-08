package qdb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// maskLengths covers the empty case, a single word, both sides of the
// word boundary, and a multi-word mask with a partial tail.
func maskLengths() []int {
	return []int{0, 1, 63, 64, 65, 1000}
}

// requireMaskMatches asserts every accessor against a []bool oracle.
func requireMaskMatches(t require.TestingT, m Mask, oracle []bool) {
	require.Equal(t, len(oracle), m.Len())
	require.Len(t, m.Bytes(), (len(oracle)+7)/8)

	nulls := 0
	for i, v := range oracle {
		require.Equal(t, v, m.IsValid(i), "slot %d", i)
		require.Equal(t, v, m.Bytes()[i>>3]>>(i&7)&1 == 1, "packed bit %d", i)
		if !v {
			nulls++
		}
	}

	require.Equal(t, nulls, m.NullCount())
	require.Equal(t, nulls == 0, m.AllValid())
	require.Equal(t, nulls == len(oracle), m.AllNull())
}

func TestMaskNewIsAllNull(t *testing.T) {
	for _, n := range maskLengths() {
		m := newMask(n)
		requireMaskMatches(t, m, make([]bool, n))
	}
}

func TestMaskSetMatchesOracle(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.SampledFrom(maskLengths()).Draw(rt, "n")
		m := newMask(n)
		oracle := make([]bool, n)

		// Random set sequence, including repeats, checked after each
		// step so a wrong word or shift is caught at the first bit.
		steps := rapid.IntRange(0, 2*n+1).Draw(rt, "steps")
		for range steps {
			if n == 0 {
				break
			}
			i := rapid.IntRange(0, n-1).Draw(rt, "i")
			m.set(i)
			oracle[i] = true
			requireMaskMatches(rt, m, oracle)
		}
	})
}

func TestMaskAllValidAfterSettingEveryBit(t *testing.T) {
	for _, n := range maskLengths() {
		m := newMask(n)
		for i := range n {
			m.set(i)
		}
		require.True(t, m.AllValid(), "n=%d", n)
		require.Equal(t, 0, m.NullCount(), "n=%d", n)
		require.Equal(t, n == 0, m.AllNull(), "n=%d", n)
	}
}
