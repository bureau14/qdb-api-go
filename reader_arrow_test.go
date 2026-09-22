package qdb

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// drainArrowStreams pulls every stream through fetchArrowStream and drain,
// returning the retained records.
func drainArrowStreams(t testHelper, r *Reader) []arrow.RecordBatch {
	t.Helper()

	var recs []arrow.RecordBatch
	for {
		stream, err := r.fetchArrowStream()
		if errors.Is(err, ErrIteratorEnd) {
			return recs
		}
		require.NoError(t, err)

		more := stream.drain(func(rec arrow.RecordBatch, err error) bool {
			require.NoError(t, err)
			recs = append(recs, rec)

			return true
		})
		require.True(t, more)
	}
}

func releaseRecords(recs []arrow.RecordBatch) {
	for _, rec := range recs {
		rec.Release()
	}
}

func TestReaderArrowStreamEmptyTableEndsImmediately(t *testing.T) {
	handle := newTestHandle(t)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, generateWriterColumnsOfAllTypes())
	require.NoError(t, err)

	reader, err := NewReader(handle, NewReaderDefaultOptions([]string{tbl.alias}))
	require.NoError(t, err)
	defer reader.Close()

	stream, err := reader.fetchArrowStream()
	assert.Nil(t, stream.reader)
	assert.ErrorIs(t, err, ErrIteratorEnd)
}

func TestReaderArrowStreamRecordsSurviveReaderRelease(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(rt)

		WithGCAndHandle(rt, handle, "TestReaderArrowStreamRecordsSurviveReaderRelease", func() {
			tables := genPopulatedTables(rt, handle)
			pushWriterTables(t, handle, tables)

			reader, err := NewReader(handle, NewReaderDefaultOptions(writerTableNames(tables)))
			require.NoError(rt, err)

			recs := drainArrowStreams(rt, &reader)
			defer releaseRecords(recs)

			reader.Close()

			total := int64(0)
			for _, rec := range recs {
				total += rec.NumRows()
				assert.Equal(rt, "$table", rec.Schema().Field(0).Name)
				assert.Equal(rt, "$timestamp", rec.Schema().Field(1).Name)
			}

			expected := int64(0)
			for _, wt := range tables {
				expected += int64(wt.RowCount())
			}
			assert.Equal(rt, expected, total)
		})
	})
}
