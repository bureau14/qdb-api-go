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
	"unsafe"
)

// :: Start - Column ::

// TsColumnType : Timeseries column types
type TsColumnType C.qdb_ts_column_type_t

// Values
//	TsColumnDouble : column is a double point
//	TsColumnBlob : column is a blob point
const (
	TsColumnUninitialized TsColumnType = C.qdb_ts_column_uninitialized
	TsColumnDouble        TsColumnType = C.qdb_ts_column_double
	TsColumnBlob          TsColumnType = C.qdb_ts_column_blob
)

type tsColumn struct {
	TsColumnInfo
	parent TimeseriesEntry
}

// TsDoubleColumn : a time series double column
type TsDoubleColumn struct {
	tsColumn
}

// TsBlobColumn : a time series blob column
type TsBlobColumn struct {
	tsColumn
}

// :: internals

func (t C.qdb_ts_column_info_t) toStructG(entry TimeseriesEntry) tsColumn {
	return tsColumn{TsColumnInfo{C.GoString(t.name), TsColumnType(t._type)}, entry}
}

func columnArrayToGo(entry TimeseriesEntry, columns *C.qdb_ts_column_info_t, columnsCount C.qdb_size_t) ([]TsDoubleColumn, []TsBlobColumn) {
	length := int(columnsCount)
	doubleColumns := []TsDoubleColumn{}
	blobColumns := []TsBlobColumn{}
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_column_info_t)(unsafe.Pointer(columns))[:length:length]
		for _, s := range tmpslice {
			if s._type == C.qdb_ts_column_double {
				doubleColumns = append(doubleColumns, TsDoubleColumn{s.toStructG(entry)})
			} else if s._type == C.qdb_ts_column_blob {
				blobColumns = append(blobColumns, TsBlobColumn{s.toStructG(entry)})
			}
		}
	}
	return doubleColumns, blobColumns
}

// :: End - Column ::

// :: Start - Column Information ::

// TsColumnInfo : column information in timeseries
type TsColumnInfo struct {
	name string
	kind TsColumnType
}

// Name : return column name
func (t TsColumnInfo) Name() string {
	return t.name
}

// Type : return column type
func (t TsColumnInfo) Type() TsColumnType {
	return t.kind
}

// NewTsColumnInfo : create a column info structure
func NewTsColumnInfo(columnName string, columnType TsColumnType) TsColumnInfo {
	return TsColumnInfo{columnName, columnType}
}

// :: internals
func (t TsColumnInfo) toStructC() C.qdb_ts_column_info_t {
	// The [4]byte is some sort of padding necessary for Go : struct(char *, int, 4 byte of padding)
	return C.qdb_ts_column_info_t{C.CString(t.name), C.qdb_ts_column_type_t(t.kind), [4]byte{}}
}

func (t C.qdb_ts_column_info_t) toStructInfoG() TsColumnInfo {
	return TsColumnInfo{C.GoString(t.name), TsColumnType(t._type)}
}

func columnInfoArrayToC(cols ...TsColumnInfo) *C.qdb_ts_column_info_t {
	if len(cols) == 0 {
		return nil
	}
	columns := make([]C.qdb_ts_column_info_t, len(cols))
	for idx, col := range cols {
		columns[idx] = col.toStructC()
	}
	return &columns[0]
}

func columnInfoArrayToGo(columns *C.qdb_ts_column_info_t, columnsCount C.qdb_size_t) []TsColumnInfo {
	length := int(columnsCount)
	columnsInfo := make([]TsColumnInfo, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_column_info_t)(unsafe.Pointer(columns))[:length:length]
		for i, s := range tmpslice {
			columnsInfo[i] = s.toStructInfoG()
		}
	}
	return columnsInfo
}

// :: End - Column Information ::