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
func newReaderDataBlob(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataBlob, error) {
	// Step 1: validation of input parameters
	if xs.data_type != C.qdb_ts_column_blob {
		return ReaderDataBlob{}, fmt.Errorf("Internal error, expected data type to be blob, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataBlob{}, fmt.Errorf("Internal error: invalid column length %d", n)
	}

	// Step 2: do a cast of xs.data[0] to *C.qdb_blob_t. As pointer sizes on different architectures
	//         may differ, *cannot* assume it's 8 bytes, and instead use `unsafe.Pointer` as the
	//         architecture-safe representation for the size.
	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	cPtr := (*C.qdb_blob_t)(rawPtr)
	if cPtr == nil {
		return ReaderDataBlob{}, fmt.Errorf("Internal error: nil data pointer for column %s", name)
	}

	// Step 3: copy data by first interpreting it as a temporary memory unsafe-slice, then copying
	//         it into a new, entirely Go-managed slice. This ensures the data can live on even when
	//         we move to a new "batch" of reader data. The actual blob content is duplicated
	//         into Go-managed byte slices.
	out := ReaderDataBlob{name: name, xs: make([][]byte, n)}
	tmp := unsafe.Slice(cPtr, n)
	for i, v := range tmp {
		out.xs[i] = C.GoBytes(unsafe.Pointer(v.content), C.int(v.content_length))
	}

	// Return result
	return out, nil
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
func newReaderDataString(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataString, error) {
	// Step 1: validation of input parameters
	if xs.data_type != C.qdb_ts_column_string {
		return ReaderDataString{}, fmt.Errorf("Internal error, expected data type to be string, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataString{}, fmt.Errorf("Internal error: invalid column length %d", n)
	}

	// Step 2: do a cast of xs.data[0] to *C.qdb_string_t. As pointer sizes on different architectures
	//         may differ, *cannot* assume it's 8 bytes, and instead use `unsafe.Pointer` as the
	//         architecture-safe representation for the size.
	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	cPtr := (*C.qdb_string_t)(rawPtr)
	if cPtr == nil {
		return ReaderDataString{}, fmt.Errorf("Internal error: nil data pointer for column %s", name)
	}

	// Step 3: copy data by first interpreting it as a temporary memory unsafe-slice, then copying
	//         it into a new, entirely Go-managed slice. This ensures the data can live on even when
	//         we move to a new "batch" of reader data. The actual string content is duplicated
	//         into Go-managed strings.
	out := ReaderDataString{name: name, xs: make([]string, n)}
	tmp := unsafe.Slice(cPtr, n)
	for i, v := range tmp {
		out.xs[i] = C.GoStringN(v.data, C.int(v.length))
	}

	// Return result
	return out, nil
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
