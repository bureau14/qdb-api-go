package qdb

/*
	#include <qdb/ts.h>
*/
import "C"
import (
	"math"
	"unsafe"
)

// :: Start - Batch ::

// TsBatch : A structure that permits to append data to a timeseries
type TsBatch struct {
	h     HandleType
	err   error
	table C.qdb_batch_table_t
}

// TsBatchColumnInfo : Represents one column in a timeseries
// Preallocate the underlying structure with the ElementCountHint
type TsBatchColumnInfo struct {
	Timeseries       string
	Column           string
	ElementCountHint int64
}

// :: internals
func (t TsBatchColumnInfo) toStructC() C.qdb_ts_batch_column_info_t {
	return C.qdb_ts_batch_column_info_t{convertToCharStar(t.Timeseries), convertToCharStar(t.Column), C.qdb_size_t(t.ElementCountHint)}
}

func (t C.qdb_ts_batch_column_info_t) toStructInfoG() TsBatchColumnInfo {
	return TsBatchColumnInfo{C.GoString(t.timeseries), C.GoString(t.column), int64(t.elements_count_hint)}
}
func tsBatchColumnInfoArrayToC(cols ...TsBatchColumnInfo) *C.qdb_ts_batch_column_info_t {
	if len(cols) == 0 {
		return nil
	}
	columns := make([]C.qdb_ts_batch_column_info_t, len(cols))
	for idx, col := range cols {
		columns[idx] = col.toStructC()
	}
	return &columns[0]
}

func batchColumnInfoArrayToSlice(columns *C.qdb_ts_batch_column_info_t, length int) []C.qdb_ts_batch_column_info_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_batch_column_info_t{})]C.qdb_ts_batch_column_info_t)(unsafe.Pointer(columns))[:length:length]
}

func releaseTsBatchColumnInfoArray(columns *C.qdb_ts_batch_column_info_t, length int) {
	if length > 0 {
		tmpslice := batchColumnInfoArrayToSlice(columns, length)
		for _, s := range tmpslice {
			releaseCharStar(s.timeseries)
			releaseCharStar(s.column)
		}
	}
}

func tsBatchColumnInfoArrayToGo(columns *C.qdb_ts_batch_column_info_t, columnsCount C.qdb_size_t) []TsBatchColumnInfo {
	length := int(columnsCount)
	columnsInfo := make([]TsBatchColumnInfo, length)
	if length > 0 {
		tmpslice := batchColumnInfoArrayToSlice(columns, length)
		for i, s := range tmpslice {
			columnsInfo[i] = s.toStructInfoG()
		}
	}
	return columnsInfo
}

// NewTsBatchColumnInfo : Creates a new TsBatchColumnInfo
func NewTsBatchColumnInfo(timeseries string, column string, hint int64) TsBatchColumnInfo {
	return TsBatchColumnInfo{timeseries, column, hint}
}
