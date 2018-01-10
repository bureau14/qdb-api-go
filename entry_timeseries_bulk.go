package qdb

/*
	#include <qdb/ts.h>

	typedef struct
	{
		double min;
		double max;
	} double_range;
*/
import "C"
import (
	"time"
)

// :: Start - Bulk ::

// TsBulk : A structure that permits to append data to a timeseries
type TsBulk struct {
	err       error
	rowCount  int
	index     int
	timestamp time.Time
	table     C.qdb_local_table_t
}

// RowCount : returns the number of rows to be append
func (t TsBulk) RowCount() int {
	return t.rowCount
}
