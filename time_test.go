package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestTimeCanConvertToQdbTimespec(t *testing.T) {
	assert := assert.New(t)

	genTime := rapid.Custom(func(t *rapid.T) time.Time {
		// Draw a random Unix second between one year ago and the distant future, *after* the year 2038
		sec := rapid.Int64Range(0, 17_179_869_184).Draw(t, "sec")

		nsec := rapid.Int64Range(0, 999_999_999).Draw(t, "nsec")
		return time.Unix(sec, nsec).UTC()
	})

	genTimes := rapid.SliceOf(genTime)

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
