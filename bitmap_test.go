package qdb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// bitmapLengths covers the empty case, a single word, both sides of the
// word boundary, and a multi-word mask with a partial tail.
func bitmapLengths() []int {
	return []int{0, 1, 63, 64, 65, 1000}
}

// requireBitmapMatches asserts every accessor against a []bool oracle.
func requireBitmapMatches(t require.TestingT, b Bitmap, oracle []bool) {
	require.Equal(t, len(oracle), b.Len())

	nulls := 0
	for i, v := range oracle {
		require.Equal(t, v, b.IsValid(i), "slot %d", i)
		if !v {
			nulls++
		}
	}

	require.Equal(t, nulls, b.NullCount())
	require.Equal(t, nulls == 0, b.AllValid())
	require.Equal(t, nulls == len(oracle), b.AllNull())
}

func TestBitmapNewIsAllNull(t *testing.T) {
	for _, n := range bitmapLengths() {
		b := newBitmap(n)
		requireBitmapMatches(t, b, make([]bool, n))
	}
}

func TestBitmapSetMatchesOracle(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.SampledFrom(bitmapLengths()).Draw(rt, "n")
		b := newBitmap(n)
		oracle := make([]bool, n)

		// Random set sequence, including repeats, checked after each
		// step so a wrong word or shift is caught at the first bit.
		steps := rapid.IntRange(0, 2*n+1).Draw(rt, "steps")
		for range steps {
			if n == 0 {
				break
			}
			i := rapid.IntRange(0, n-1).Draw(rt, "i")
			b.set(i)
			oracle[i] = true
			requireBitmapMatches(rt, b, oracle)
		}
	})
}

func TestBitmapAllValidAfterSettingEveryBit(t *testing.T) {
	for _, n := range bitmapLengths() {
		b := newBitmap(n)
		for i := range n {
			b.set(i)
		}
		require.True(t, b.AllValid(), "n=%d", n)
		require.Equal(t, 0, b.NullCount(), "n=%d", n)
		require.Equal(t, n == 0, b.AllNull(), "n=%d", n)
	}
}
