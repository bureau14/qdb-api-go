// Package qdb provides an api to a quasardb server
package qdb

/*
        #include <string.h> // for memcpy
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"
import (
	"fmt"
	"time"
	"unsafe"
)

type ReaderData interface {
	// Returns the column name
	Name() string

	// Possibly some methods, but often empty
	valueType() TsValueType

	// Release any C allocated buffers managed
	release(h HandleType) error
}

// Int64
type ReaderDataInt64 struct {
	name string
	xs   []int64
}

func (rd *ReaderDataInt64) Name() string {
	return rd.name
}

func (rd *ReaderDataInt64) Data() []int64 {
	return rd.xs
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is int64,returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataInt64(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataInt64, error) {
	// Step 1: validation of input parameters
	if xs.data_type != C.qdb_ts_column_int64 {
		return ReaderDataInt64{}, fmt.Errorf("Internal error, expected data type to be int64, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataInt64{}, fmt.Errorf("Internal error: invalid column length %d", n)
	}

	// Step 2: do a cast of xs.data[0] to *C.qdb_int_t. As pointer sizes on different architectures
	//         may differ, *cannot* assume it's 8 bytes, and instead use `unsafe.Pointer` as the
	//         architecture-safe representation for the size.
	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	cPtr := (*C.qdb_int_t)(rawPtr)
	if cPtr == nil {
		return ReaderDataInt64{}, fmt.Errorf("Internal error: nil data pointer for column %s", name)
	}

	// Step 3: copy data by first interpreting it as a temporary memory unsafe-slice, then copying
	//         it into a new, entirely Go-managed slice. This ensures the data can live on even when
	//         we move to a new "batch" of reader data.
	out := ReaderDataInt64{name: name, xs: make([]int64, n)}
	tmp := unsafe.Slice(cPtr, n)
	for i, v := range tmp {
		out.xs[i] = int64(v)
	}

	// Return result
	return out, nil
}

// Double
type ReaderDataDouble struct {
	name string
	xs   []float64
}

func (rd *ReaderDataDouble) Name() string {
	return rd.name
}

func (rd *ReaderDataDouble) Data() []float64 {
	return rd.xs
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is double,returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataDouble(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataDouble, error) {
	// Step 1: validation of input parameters
	if xs.data_type != C.qdb_ts_column_double {
		return ReaderDataDouble{}, fmt.Errorf("Internal error, expected data type to be double, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataDouble{}, fmt.Errorf("Internal error: invalid column length %d", n)
	}

	// Step 2: do a cast of xs.data[0] to *C.double. As pointer sizes on different architectures
	//         may differ, *cannot* assume it's 8 bytes, and instead use `unsafe.Pointer` as the
	//         architecture-safe representation for the size.
	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	cPtr := (*C.double)(rawPtr)
	if cPtr == nil {
		return ReaderDataDouble{}, fmt.Errorf("Internal error: nil data pointer for column %s", name)
	}

	// Step 3: copy data by first interpreting it as a temporary memory unsafe-slice, then copying
	//         it into a new, entirely Go-managed slice. This ensures the data can live on even when
	//         we move to a new "batch" of reader data.
	out := ReaderDataDouble{name: name, xs: make([]float64, n)}
	tmp := unsafe.Slice(cPtr, n)
	for i, v := range tmp {
		out.xs[i] = float64(v)
	}

	// Return result
	return out, nil
}

// Timestamp
type ReaderDataTimestamp struct {
	name string
	xs   []time.Time
}

func (rd *ReaderDataTimestamp) Name() string {
	return rd.name
}

func (rd *ReaderDataTimestamp) Data() []time.Time {
	return rd.xs
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is timestamp, returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataTimestamp(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataTimestamp, error) {
	// Step 1: validation of input parameters
	if xs.data_type != C.qdb_ts_column_timestamp {
		return ReaderDataTimestamp{}, fmt.Errorf("Internal error, expected data type to be timestamp, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataTimestamp{}, fmt.Errorf("Internal error: invalid column length %d", n)
	}

	// Step 2: do a cast of xs.data[0] to *C.qdb_timespec_t. As pointer sizes on different architectures
	//         may differ, *cannot* assume it's 8 bytes, and instead use `unsafe.Pointer` as the
	//         architecture-safe representation for the size.
	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	cPtr := (*C.qdb_timespec_t)(rawPtr)
	if cPtr == nil {
		return ReaderDataTimestamp{}, fmt.Errorf("Internal error: nil data pointer for column %s", name)
	}

	// Step 3: copy data by first interpreting it as a temporary memory unsafe-slice, then copying
	//         it into a new, entirely Go-managed slice. This ensures the data can live on even when
	//         we move to a new "batch" of reader data.
	out := ReaderDataTimestamp{name: name, xs: make([]time.Time, n)}
	tmp := unsafe.Slice(cPtr, n)
	for i, v := range tmp {
		out.xs[i] = QdbTimespecToTime(v)
	}

	// Return result
	return out, nil
}

// Blob
type ReaderDataBlob struct {
	name string
	xs   [][]byte
}

func (rd *ReaderDataBlob) Name() string {
	return rd.name
}

func (rd *ReaderDataBlob) Data() [][]byte {
	return rd.xs
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is blob, returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataBlob(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataTimestamp, error) {
	// TODO: complete with the exact same commment structure and code structure as newReaderDataInt64, with
	//       the addition that an additional copy of data must happen for the actual data within the blobs.
	//       That is, the `qdb_blob_t` structures must be copied into `[]byte` structures in a memory-safe
	//       way.
}

// String
type ReaderDataString struct {
	name string
	xs   []string
}

func (rd *ReaderDataString) Name() string {
	return rd.name
}

func (rd *ReaderDataString) Data() []string {
	return rd.xs
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is string, returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataString(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataTimestamp, error) {
	// TODO: complete with the exact same commment structure and code structure as newReaderDataInt64, with
	//       the addition that an additional copy of data must happen for the actual data within the strings.
	//       That is, the `qdb_string_t` structures must be copied into `string` structures in a memory-safe
	//       way.
}

// Metadata we need to represent a single column.
type ReaderColumn struct {
	columnName string
	columnType TsColumnType
}

func (rc ReaderColumn) Name() string {
	return rc.columnName
}

func (rc ReaderColumn) Type() TsColumnType {
	return rc.columnType
}

type ReaderTable struct {
	// Name of the table this data is for
	tableName string

	// All arrays are guaranteed to be of lenght `rowCount`. This means specifically
	// the `idx` parameter and all Writerdata value arrays within `data`.
	rowCount int

	// An index that enables looking up of a column's name by its offset within the table.
	columnInfoByOffset []ReaderColumn

	// The index, can not contain null values
	idx []time.Time

	// Value arrays read from each column
	data []ReaderData
}

// Returns name of the table
func (rt *ReaderTable) TableName() string {
	return rt.tableName
}

// Returns number of rows in this chunk / table
func (rt *ReaderTable) RowCount() string {
	return rt.tableName
}

// Creates new ReaderTable object out of a qdb_exp_batch_push_table_t struct. Memory-safe,
// in that it copies all the memory which means these objects are safe to use for a long time.
//
// As all schemas for all tables are required to be the same, this function accepts the `columns`
// parameter that were parsed earlier. For convenience, in our case, we set it as part of each
// ReaderTable object.
func newReaderTable(columns []ReaderColumn, tbl qdb_exp_batch_push_table_t) (ReaderTable, error) {

	// Step 1: input validation
	// TODO: return error if `tbl.name` is nil
	// TODO: return error if `tbl.data.timestamps` is nil
	// TODO: return error if `tbl.data.columns` is nil
	// TODO: return error if `tbl.data.row_count` is not > 0
	// TODO: return error if `tbl.data.column_count` is not > 0

	var out ReaderTable
	out.columnInfoByOffset = columns

	// TODO: set out.tableName from tbl.name, copy memory

	// We mostly care about the actual data and will be using that struct a lot, so let's
	// acquire a reference to it.
	var data *C.qdb_exp_batch_push_table_data_t = &(tbl.data)

	// TODO: set out.rowCount based on data.row_count
	// TODO: set out.idx based on data.timestamps. use the rowCount to know how long the slice should be

	// TODO: store the `data` object by:
	//  - iterating over all the `data.columns`, store in variable name `column`
	//  - add check that `column.data_type` matches the `columns[i].ValueType()`, this would be an internal error if not the case
	//  - dispatch to correct `newReaderData...` function based on the ValueType() and store it in out.data[i]

	return out, nil
}
