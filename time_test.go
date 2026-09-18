package qdb

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestTimeCanConvertToQdbTimespec(t *testing.T) {
	assert := assert.New(t)

	genTimes := rapid.SliceOf(rapid.Custom(genTime))

	rapid.Check(t, func(t *rapid.T) {
		// Generate a random set of times
		input := genTimes.Draw(t, "times")

		// Assert that every generated time is in UTC
		for _, tm := range input {
			assert.Equal(time.UTC, tm.Location(), "generated time should be in UTC")
		}

		// Convert to qdb_timespec_t and back to time, doing the reconversion
		output := QdbTimespecSliceToTime(TimeSliceToQdbTimespec(input))

		assert.Equal(input, output)
	})
}

// A null in a timestamp column is qdb_min_time in both timespec fields, which
// time.Time normalises away; NullTime has to map onto it and back.
func TestNullTimeConvertsToNullTimespec(t *testing.T) {
	assert := assert.New(t)

	input := []time.Time{time.Unix(1, 2).UTC(), NullTime(), {}}
	native := TimeSliceToQdbTimespec(input)

	assert.False(isNullTimespec(native[0]))
	assert.True(isNullTimespec(native[1]))
	assert.False(isNullTimespec(native[2]), "the zero time is a value, not a null")

	output := QdbTimespecSliceToTime(native)
	assert.Equal(input[0], output[0])
	assert.True(IsNullTime(output[1]))
	assert.Equal(NullTime(), output[1])
}

func TestIsNullTime(t *testing.T) {
	assert := assert.New(t)

	assert.True(IsNullTime(NullTime()))
	assert.True(IsNullTime(NullTime().Local()))
	assert.True(IsNullTime(time.Unix(0, math.MinInt64)))
	assert.False(IsNullTime(time.Time{}))
	assert.False(IsNullTime(NullTime().Add(time.Nanosecond)))
}
