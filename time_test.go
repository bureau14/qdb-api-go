package qdb

import (
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
