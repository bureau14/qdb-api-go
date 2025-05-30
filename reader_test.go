package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderOptionsCanCreateNew(t *testing.T) {
	assert := assert.New(t)

	// TODO: implement test case that ensure we can create a ReaderOptions object in the first place
}

func TestReaderOptionsCanSetProperties(t *testing.T) {
	assert := assert.New(t)

	// TODO: implement test cases for ReaderOptions:
	//   - verify that we can set tables using WithTables()
	//   - verify that we can set columns using WithColumns()
	//   - verify that we can set the time range using WithTimeRange()
	//   - verify that we can disable the time range using WithoutTimeRange()
	//
	// Each test should assert that the values inside `ReaderOptions` are actually set correctly

}

func TestReaderReturnsErrorOnInvalidRange(t *testing.T) {
	assert := assert.New(t)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(err)
	defer handle.Close()

	// TODO: implements test case for NewReader() that ensures that an error is returned if
	//       either no range is set at all, or the range end is not after the range start.
}
