package qdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------
// qdb_query_arrow binding
// ---------------------------------------------------------------------

func TestExecuteArrowBinding(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 16, 50)

	t.Run("select yields one column per fixture column", func(t *testing.T) {
		r, err := handle.Query(selectFixture(td)).executeArrow()
		require.NoError(t, err)
		require.NotNil(t, r)
		defer r.Close()

		cols := r.columns()
		require.Len(t, cols, 8)
		// The schema name is the query's column name; the fixture selects *,
		// so the C side fills in the table's column names.
		assert.Equal(t, "$timestamp", arrowColumnName(&cols[rsTimestampIndex]))
		assert.Equal(t, "$table", arrowColumnName(&cols[rsTableIndex]))
		for i := range cols {
			assert.False(t, arrowColumnMoved(&cols[i]), "column %d", i)
		}
	})

	t.Run("ddl yields a nil result", func(t *testing.T) {
		alias := generateAlias(16)
		r, err := handle.Query(fmt.Sprintf("create table %s ($timestamp TIMESTAMP, id INT64)", alias)).executeArrow()
		require.NoError(t, err)
		assert.Nil(t, r)

		r, err = handle.Query(fmt.Sprintf("drop table %s", alias)).executeArrow()
		require.NoError(t, err)
		assert.Nil(t, r)
	})

	t.Run("invalid query yields an error and no result", func(t *testing.T) {
		r, err := handle.Query("SELECT FROM").executeArrow()
		require.ErrorIs(t, err, ErrInvalidQuery)
		assert.Nil(t, r)
	})

	t.Run("close is nil-safe and idempotent", func(t *testing.T) {
		var nilResult *queryArrowResult
		nilResult.Close()
		assert.Nil(t, nilResult.columns())

		r, err := handle.Query(selectFixture(td)).executeArrow()
		require.NoError(t, err)
		r.Close()
		r.Close()
		assert.Nil(t, r.columns())
	})
}
