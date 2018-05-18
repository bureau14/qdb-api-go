package qdb

/*
	#include <qdb/ts.h>
*/
import "C"
import "unsafe"

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
func tsBtachColumnInfoArrayToC(cols ...TsBatchColumnInfo) *C.qdb_ts_batch_column_info_t {
	if len(cols) == 0 {
		return nil
	}
	columns := make([]C.qdb_ts_batch_column_info_t, len(cols))
	for idx, col := range cols {
		columns[idx] = col.toStructC()
	}
	return &columns[0]
}

func releaseTsBtachColumnInfoArray(columns *C.qdb_ts_batch_column_info_t, length int) {
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_batch_column_info_t)(unsafe.Pointer(columns))[:length:length]
		for _, s := range tmpslice {
			releaseCharStar(s.timeseries)
			releaseCharStar(s.column)
		}
	}
}

func tsBtachColumnInfoArrayToGo(columns *C.qdb_ts_batch_column_info_t, columnsCount C.qdb_size_t) []TsBatchColumnInfo {
	length := int(columnsCount)
	columnsInfo := make([]TsBatchColumnInfo, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_batch_column_info_t)(unsafe.Pointer(columns))[:length:length]
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
