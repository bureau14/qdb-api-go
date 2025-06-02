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

	// returns the type of data for this column
	valueType() TsValueType
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

func (rd *ReaderDataInt64) valueType() TsValueType {
	return TsValueInt64
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

// GetReaderDataInt64 safely extracts a slice of int64 values from a ReaderData object.
//
// Returns an error if the underlying type is not ReaderDataInt64.
func GetReaderDataInt64(rd ReaderData) ([]int64, error) {
	v, ok := rd.(*ReaderDataInt64)
	if !ok {
		return nil, fmt.Errorf("GetReaderDataInt64: type mismatch, expected ReaderDataInt64, got %T", rd)
	}
	return v.xs, nil
}

// GetReaderDataInt64Unsafe is the unsafe variant of GetReaderDataInt64. Undefined behavior
// occurs when invoked on a ReaderData of the incorrect concrete type.
func GetReaderDataInt64Unsafe(rd ReaderData) []int64 {
	return (*ReaderDataInt64)(ifaceDataPtr(rd)).xs
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

func (rd *ReaderDataDouble) valueType() TsValueType {
	return TsValueDouble
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

// GetReaderDataDouble safely extracts a slice of float64 values from a ReaderData object.
//
// Returns an error if the underlying type is not ReaderDataDouble.
func GetReaderDataDouble(rd ReaderData) ([]float64, error) {
	v, ok := rd.(*ReaderDataDouble)
	if !ok {
		return nil, fmt.Errorf("GetReaderDataDouble: type mismatch, expected ReaderDataDouble, got %T", rd)
	}
	return v.xs, nil
}

// GetReaderDataDoubleUnsafe is the unsafe variant of GetReaderDataDouble. Undefined behavior
// occurs when invoked on a ReaderData of the incorrect concrete type.
func GetReaderDataDoubleUnsafe(rd ReaderData) []float64 {
	return (*ReaderDataDouble)(ifaceDataPtr(rd)).xs
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

func (rd *ReaderDataTimestamp) valueType() TsValueType {
	return TsValueTimestamp
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

// GetReaderDataTimestamp safely extracts a slice of time.Time values from a ReaderData object.
//
// Returns an error if the underlying type is not ReaderDataTimestamp.
func GetReaderDataTimestamp(rd ReaderData) ([]time.Time, error) {
	v, ok := rd.(*ReaderDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetReaderDataTimestamp: type mismatch, expected ReaderDataTimestamp, got %T", rd)
	}
	return v.xs, nil
}

// GetReaderDataTimestampUnsafe is the unsafe variant of GetReaderDataTimestamp. Undefined behavior
// occurs when invoked on a ReaderData of the incorrect concrete type.
func GetReaderDataTimestampUnsafe(rd ReaderData) []time.Time {
	return (*ReaderDataTimestamp)(ifaceDataPtr(rd)).xs
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

func (rd *ReaderDataBlob) valueType() TsValueType {
	return TsValueBlob
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

// GetReaderDataBlob safely extracts a slice of byte slices from a ReaderData object.
//
// Returns an error if the underlying type is not ReaderDataBlob.
func GetReaderDataBlob(rd ReaderData) ([][]byte, error) {
	v, ok := rd.(*ReaderDataBlob)
	if !ok {
		return nil, fmt.Errorf("GetReaderDataBlob: type mismatch, expected ReaderDataBlob, got %T", rd)
	}
	return v.xs, nil
}

// GetReaderDataBlobUnsafe is the unsafe variant of GetReaderDataBlob. Undefined behavior
// occurs when invoked on a ReaderData of the incorrect concrete type.
func GetReaderDataBlobUnsafe(rd ReaderData) [][]byte {
	return (*ReaderDataBlob)(ifaceDataPtr(rd)).xs
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

func (rd *ReaderDataString) valueType() TsValueType {
	return TsValueString
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

// GetReaderDataString safely extracts a slice of strings from a ReaderData object.
//
// Returns an error if the underlying type is not ReaderDataString.
func GetReaderDataString(rd ReaderData) ([]string, error) {
	v, ok := rd.(*ReaderDataString)
	if !ok {
		return nil, fmt.Errorf("GetReaderDataString: type mismatch, expected ReaderDataString, got %T", rd)
	}
	return v.xs, nil
}

// GetReaderDataStringUnsafe is the unsafe variant of GetReaderDataString. Undefined behavior
// occurs when invoked on a ReaderData of the incorrect concrete type.
func GetReaderDataStringUnsafe(rd ReaderData) []string {
	return (*ReaderDataString)(ifaceDataPtr(rd)).xs
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
func newReaderTable(columns []ReaderColumn, tbl C.qdb_exp_batch_push_table_t) (ReaderTable, error) {

	// Step 1: input validation
	if tbl.name == nil {
		return ReaderTable{}, fmt.Errorf("Internal error: nil table name")
	}

	if tbl.data.timestamps == nil {
		return ReaderTable{}, fmt.Errorf("Internal error: nil timestamps for table %s", C.GoString(tbl.name))
	}

	if tbl.data.columns == nil {
		return ReaderTable{}, fmt.Errorf("Internal error: nil columns for table %s", C.GoString(tbl.name))
	}

	if tbl.data.row_count <= 0 {
		return ReaderTable{}, fmt.Errorf("Internal error: invalid row count %d", tbl.data.row_count)
	}

	if tbl.data.column_count <= 0 {
		return ReaderTable{}, fmt.Errorf("Internal error: invalid column count %d", tbl.data.column_count)
	}

	var out ReaderTable
	out.columnInfoByOffset = columns

	// Copy table name from C memory
	out.tableName = C.GoString(tbl.name)

	// We mostly care about the actual data and will be using that struct a lot, so let's
	// acquire a reference to it.
	var data *C.qdb_exp_batch_push_table_data_t = &(tbl.data)

	// Set row count
	out.rowCount = int(data.row_count)

	// Copy index
	idxSlice := unsafe.Slice(data.timestamps, out.rowCount)
	out.idx = make([]time.Time, out.rowCount)
	for i, v := range idxSlice {
		out.idx[i] = QdbTimespecToTime(v)
	}

	// Store the column data
	colCount := int(data.column_count)
	out.data = make([]ReaderData, colCount)

	columnSlice := unsafe.Slice(data.columns, colCount)
	for i := 0; i < colCount; i++ {
		column := columnSlice[i]

		expected := C.qdb_ts_column_type_t(columns[i].columnType)
		if column.data_type != expected {
			return ReaderTable{}, fmt.Errorf("Internal error: column %d type mismatch (expected %v, got %v)", i, expected, column.data_type)
		}

		name := columns[i].columnName

		switch columns[i].columnType.AsValueType() {
		case TsValueInt64:
			v, err := newReaderDataInt64(name, column, out.rowCount)
			if err != nil {
				return ReaderTable{}, err
			}
			out.data[i] = &v
		case TsValueDouble:
			v, err := newReaderDataDouble(name, column, out.rowCount)
			if err != nil {
				return ReaderTable{}, err
			}
			out.data[i] = &v
		case TsValueTimestamp:
			v, err := newReaderDataTimestamp(name, column, out.rowCount)
			if err != nil {
				return ReaderTable{}, err
			}
			out.data[i] = &v
		case TsValueBlob:
			v, err := newReaderDataBlob(name, column, out.rowCount)
			if err != nil {
				return ReaderTable{}, err
			}
			out.data[i] = &v
		case TsValueString:
			v, err := newReaderDataString(name, column, out.rowCount)
			if err != nil {
				return ReaderTable{}, err
			}
			out.data[i] = &v
		default:
			return ReaderTable{}, fmt.Errorf("Internal error: unsupported value type for column %s", name)
		}
	}

	return out, nil
}

type ReaderOptions struct {
	tables     []string
	columns    []string
	rangeStart time.Time
	rangeEnd   time.Time
}

func NewReaderOptions() ReaderOptions {
	return ReaderOptions{}
}

// Creates ReaderOptions object that fetches all data for all columns for a set of tables
func NewReaderDefaultOptions(tables []string) ReaderOptions {
	return NewReaderOptions().WithTables(tables)
}

func (ro ReaderOptions) WithTables(tables []string) ReaderOptions {
	ro.tables = tables
	return ro
}

func (ro ReaderOptions) WithColumns(columns []string) ReaderOptions {
	ro.columns = columns
	return ro
}

func (ro ReaderOptions) WithTimeRange(start time.Time, end time.Time) ReaderOptions {
	ro.rangeStart = start
	ro.rangeEnd = end

	return ro
}

// Holds the current state of the reader
type Reader struct {
	// Options that were used to initialize the reader, for future reference if required
	options ReaderOptions

	// Current state of the reader handle.
	state C.qdb_reader_handle_t
}

// Initialize a new bulk reader. This performs the initialization of the bulk reader, and does not yet fetch data.
// The user is expected to invoke `Reader.Close()` to release allocated memory.
func NewReader(h HandleType, options ReaderOptions) (Reader, error) {
	var ret Reader
	ret.options = options

	// Step 1: validation of options
	//  - at least one table must be specified
	//  - a valid time range must be provided
	if len(options.tables) == 0 {
		return ret, fmt.Errorf("no tables provided")
	}

	// Validate the time range arguments.
	// Either both rangeStart and rangeEnd must be zero (meaning no range
	// filtering) or both must be non-zero.  Having only one of them set is
	// invalid.
	if options.rangeStart.IsZero() != options.rangeEnd.IsZero() {
		return ret, fmt.Errorf("invalid time range")
	}

	if options.rangeEnd.IsZero() == false && !options.rangeEnd.After(options.rangeStart) {
		return ret, fmt.Errorf("invalid time range")
	}

	// Step 2: convert the options.rangeStart / options.rangeEnd to qdb_ts_range_t
	var cRanges *C.qdb_ts_range_t = nil
	var cRangeCount int = 0

	if options.rangeStart.IsZero() == false && options.rangeEnd.IsZero() == false {
		// There is exactly one range. Allocate memory for it via qdbAllocBuffer
		// and convert the start and end times to the native representation.
		ptr, err := qdbAllocBuffer[C.qdb_ts_range_t](h, 1)
		if err != nil {
			return ret, fmt.Errorf("failed to allocate time range: %w", err)
		}
		defer qdbRelease(h, ptr)

		slice := unsafe.Slice(ptr, 1)
		slice[0].begin = toQdbTimespec(options.rangeStart)
		slice[0].end = toQdbTimespec(options.rangeEnd)

		cRanges = ptr
		cRangeCount = 1
	}

	// Step 3: Initialize `C.qdb_bulk_reader_table_t` structs. Even though our C API allows for using
	// different ranges per table, we use a fixed range for all tables. As such, we can reuse the
	// previously constructed time ranges for all tables.  The table array itself is allocated via
	// the QuasarDB allocator so it is compatible with the C API.  Each table name must also be
	// allocated using qdbCopyString and is immediately deferred for release.
	tableCount := len(options.tables)
	cTables, err := qdbAllocBuffer[C.qdb_bulk_reader_table_t](h, tableCount)
	if err != nil {
		return ret, fmt.Errorf("failed to allocate table array: %w", err)
	}
	defer qdbRelease(h, cTables)
	tblSlice := unsafe.Slice(cTables, tableCount)

	for i, tbl := range options.tables {
		name, err := qdbCopyString(h, tbl)
		if err != nil {
			// release any previously allocated names before returning
			for j := 0; j < i; j++ {
				if tblSlice[j].name != nil {
					qdbRelease(h, tblSlice[j].name)
				}
			}
			return ret, fmt.Errorf("failed to copy table name: %w", err)
		}
		defer qdbRelease(h, name)

		tblSlice[i].name = name
		tblSlice[i].ranges = cRanges
		tblSlice[i].range_count = C.qdb_size_t(cRangeCount)
	}

	// Step 4: Initialize `columns` arguments.  Column names are optional.  If provided we allocate
	// an array of char* pointers and copy each string using qdbCopyString.  The allocated memory is
	// freed immediately after calling into the C API.
	columnCount := len(options.columns)
	var cColumns **C.char
	if columnCount > 0 {
		ptr, err := qdbAllocBuffer[*C.char](h, columnCount)
		if err != nil {
			return ret, fmt.Errorf("failed to allocate column array: %w", err)
		}
		defer qdbRelease(h, ptr)
		colSlice := unsafe.Slice(ptr, columnCount)
		for i, col := range options.columns {
			cname, err := qdbCopyString(h, col)
			if err != nil {
				return ret, fmt.Errorf("failed to copy column name: %w", err)
			}
			defer qdbRelease(h, cname)
			colSlice[i] = cname
		}
		cColumns = ptr
	} else {
		cColumns = nil
	}

	// Step 5: invoke qdb_bulk_reader_fetch() which initializes the native reader handle.  After
	// this call the C API manages its own copy of the provided tables and columns so we can
	// safely release our temporary allocations via the deferred qdbRelease calls.
	var readerHandle C.qdb_reader_handle_t
	errCode := C.qdb_bulk_reader_fetch(
		h.handle,
		cColumns,
		C.qdb_size_t(columnCount),
		cTables,
		C.qdb_size_t(tableCount),
		&readerHandle,
	)
	if err := makeErrorOrNil(errCode); err != nil {
		return ret, err
	}

	ret.state = readerHandle

	// Done, return state.

	return ret, nil
}

// Releases underlying memory
func (r *Reader) Close(h HandleType) {
	// if state is non-nil, invoke qdbRelease() on state
	if r.state != nil {
		qdbReleasePointer(h, unsafe.Pointer(r.state))
		r.state = nil
	}

}
