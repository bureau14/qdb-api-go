// Package qdb provides an api to a quasardb server
package qdb

/*
   #include <stdlib.h>
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

	// Ensures the underlying data pre-allocates a certain capacity, useful when we know
	// we will have multiple incremental allocations.
	ensureCapacity(n int)

	// Appends another chunk of `ReaderData` to this. Validates that they are of identical types.
	// The intent of this function is the "merge" multiple chunks of ReaderData together in case
	// the user calls FetchAll().
	//
	// Returns error if:
	//  * Name() does not match
	//  * ReaderData is not of the same type as the struct implementing this function.
	appendData(data ReaderData) error

	// Unsafe variant of appendData(), undefined behavior in case it's used incorrectly
	appendDataUnsafe(data ReaderData)
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

func (rd *ReaderDataInt64) ensureCapacity(n int) {
	if cap(rd.xs) < n {
		tmp := make([]int64, len(rd.xs), n)
		copy(tmp, rd.xs)
		rd.xs = tmp
	}
}

func (rd *ReaderDataInt64) appendData(data ReaderData) error {
	if rd.name != data.Name() {
		return fmt.Errorf("name mismatch: expected '%s', got '%s'", rd.name, data.Name())
	}

	other, ok := data.(*ReaderDataInt64)
	if !ok {
		return fmt.Errorf("appendData: type mismatch, expected ReaderDataInt64, got %T", data)
	}

	rd.xs = append(rd.xs, other.xs...)
	return nil
}

func (rd *ReaderDataInt64) appendDataUnsafe(data ReaderData) {
	other := (*ReaderDataInt64)(ifaceDataPtr(data))
	rd.xs = append(rd.xs, other.xs...)
}

func newReaderDataInt64(name string, xs []int64) ReaderDataInt64 {
	return ReaderDataInt64{name: name, xs: xs}
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is int64,returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataInt64FromNative(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataInt64, error) {

	// Ensure the column type matches int64; mismatches lead to invalid reads.
	if xs.data_type != C.qdb_ts_column_int64 {
		return ReaderDataInt64{},
			fmt.Errorf("newReaderDataInt64FromNative: expected data_type int64, got %v", xs.data_type)
	}
	// Reject non-positive lengths; indexing with n <= 0 is invalid.
	if n <= 0 {
		return ReaderDataInt64{},
			fmt.Errorf("newReaderDataInt64FromNative: invalid column length %d", n)
	}

	// Extract the raw C pointer from xs.data[0] (void* array). Casting via unsafe.Pointer
	// ensures portability across architectures (pointer size differences).
	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if rawPtr == nil {
		return ReaderDataInt64{},
			fmt.Errorf("newReaderDataInt64FromNative: nil data pointer for column %q", name)
	}

	// SAFELY convert the C pointer array to a fully Go-managed []int64 slice.
	// cPointerArrayToSlice enforces:
	//   - runtime check that sizeof(C.qdb_int_t) == sizeof(int64)
	//   - bounds validation on length
	//   - explicit copy from C memory into Go memory
	// This guarantees memory safety even if the original C buffer is freed afterward.
	goSlice, err := cPointerArrayToSlice[C.qdb_int_t, int64](rawPtr, int64(n))
	if err != nil {
		return ReaderDataInt64{}, fmt.Errorf("newReaderDataInt64FromNative: %v", err)
	}

	// Wrap the Go-managed []int64 in a ReaderDataInt64 and return.
	// Using newReaderDataInt64 centralizes ReaderDataInt64 construction logic.
	return newReaderDataInt64(name, goSlice), nil
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

func (rd *ReaderDataDouble) ensureCapacity(n int) {
	if cap(rd.xs) < n {
		tmp := make([]float64, len(rd.xs), n)
		copy(tmp, rd.xs)
		rd.xs = tmp
	}
}

func (rd *ReaderDataDouble) appendData(data ReaderData) error {
	if rd.name != data.Name() {
		return fmt.Errorf("name mismatch: expected '%s', got '%s'", rd.name, data.Name())
	}

	other, ok := data.(*ReaderDataDouble)
	if !ok {
		return fmt.Errorf("appendData: type mismatch, expected ReaderDataDouble, got %T", data)
	}

	rd.xs = append(rd.xs, other.xs...)
	return nil
}

func (rd *ReaderDataDouble) appendDataUnsafe(data ReaderData) {
	other := (*ReaderDataDouble)(ifaceDataPtr(data))
	rd.xs = append(rd.xs, other.xs...)
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is double,returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataDouble(name string, xs []float64) ReaderDataDouble {
	return ReaderDataDouble{name: name, xs: xs}
}

// newReaderDataDoubleFromNative converts the native C representation to a Go-managed
// ReaderDataDouble. Memory from the C layer is copied into Go slices.
//
// Assumes `xs.data_type` is double, returns error otherwise.
func newReaderDataDoubleFromNative(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataDouble, error) {
	if xs.data_type != C.qdb_ts_column_double {
		return ReaderDataDouble{}, fmt.Errorf("newReaderDataDoubleFromNative: expected data type double, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataDouble{}, fmt.Errorf("newReaderDataDoubleFromNative: invalid column length %d", n)
	}

	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if rawPtr == nil {
		return ReaderDataDouble{}, fmt.Errorf("newReaderDataDoubleFromNative: nil data pointer for column %s", name)
	}

	goSlice, err := cPointerArrayToSlice[C.double, float64](rawPtr, int64(n))
	if err != nil {
		return ReaderDataDouble{}, fmt.Errorf("newReaderDataDoubleFromNative: %v", err)
	}

	return newReaderDataDouble(name, goSlice), nil
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

func (rd *ReaderDataTimestamp) ensureCapacity(n int) {
	if cap(rd.xs) < n {
		tmp := make([]time.Time, len(rd.xs), n)
		copy(tmp, rd.xs)
		rd.xs = tmp
	}
}

func (rd *ReaderDataTimestamp) appendData(data ReaderData) error {
	if rd.name != data.Name() {
		return fmt.Errorf("name mismatch: expected '%s', got '%s'", rd.name, data.Name())
	}

	other, ok := data.(*ReaderDataTimestamp)
	if !ok {
		return fmt.Errorf("appendData: type mismatch, expected ReaderDataTimestamp, got %T", data)
	}

	rd.xs = append(rd.xs, other.xs...)
	return nil
}

func (rd *ReaderDataTimestamp) appendDataUnsafe(data ReaderData) {
	other := (*ReaderDataTimestamp)(ifaceDataPtr(data))
	rd.xs = append(rd.xs, other.xs...)
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is timestamp, returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataTimestamp(name string, xs []time.Time) ReaderDataTimestamp {
	return ReaderDataTimestamp{name: name, xs: xs}
}

// newReaderDataTimestampFromNative converts the native C representation to a Go-managed
// ReaderDataTimestamp, copying the C memory into Go slices.
func newReaderDataTimestampFromNative(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataTimestamp, error) {
	if xs.data_type != C.qdb_ts_column_timestamp {
		return ReaderDataTimestamp{}, fmt.Errorf("newReaderDataTimestampFromNative: expected data type timestamp, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataTimestamp{}, fmt.Errorf("newReaderDataTimestampFromNative: invalid column length %d", n)
	}

	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if rawPtr == nil {
		return ReaderDataTimestamp{}, fmt.Errorf("newReaderDataTimestampFromNative: nil data pointer for column %s", name)
	}

	tsSlice, err := cPointerArrayToSlice[C.qdb_timespec_t, C.qdb_timespec_t](rawPtr, int64(n))
	if err != nil {
		return ReaderDataTimestamp{}, fmt.Errorf("newReaderDataTimestampFromNative: %v", err)
	}

	goTimes := QdbTimespecSliceToTime(tsSlice)
	return newReaderDataTimestamp(name, goTimes), nil
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

func (rd *ReaderDataBlob) ensureCapacity(n int) {
	if cap(rd.xs) < n {
		tmp := make([][]byte, len(rd.xs), n)
		copy(tmp, rd.xs)
		rd.xs = tmp
	}
}

func (rd *ReaderDataBlob) appendData(data ReaderData) error {
	if rd.name != data.Name() {
		return fmt.Errorf("name mismatch: expected '%s', got '%s'", rd.name, data.Name())
	}

	other, ok := data.(*ReaderDataBlob)
	if !ok {
		return fmt.Errorf("appendData: type mismatch, expected ReaderDataBlob, got %T", data)
	}

	rd.xs = append(rd.xs, other.xs...)
	return nil
}

func (rd *ReaderDataBlob) appendDataUnsafe(data ReaderData) {
	other := (*ReaderDataBlob)(ifaceDataPtr(data))
	rd.xs = append(rd.xs, other.xs...)
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is blob, returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataBlob(name string, xs [][]byte) ReaderDataBlob {
	return ReaderDataBlob{name: name, xs: xs}
}

// newReaderDataBlobFromNative converts the C.qdb_exp_batch_push_column_t blob representation
// to a Go-managed ReaderDataBlob.
func newReaderDataBlobFromNative(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataBlob, error) {
	if xs.data_type != C.qdb_ts_column_blob {
		return ReaderDataBlob{}, fmt.Errorf("newReaderDataBlobFromNative: expected data type blob, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataBlob{}, fmt.Errorf("newReaderDataBlobFromNative: invalid column length %d", n)
	}

	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if rawPtr == nil {
		return ReaderDataBlob{}, fmt.Errorf("newReaderDataBlobFromNative: nil data pointer for column %s", name)
	}

	blobs, err := cPointerArrayToSlice[C.qdb_blob_t, C.qdb_blob_t](rawPtr, int64(n))
	if err != nil {
		return ReaderDataBlob{}, fmt.Errorf("newReaderDataBlobFromNative: %v", err)
	}

	outSlice := make([][]byte, n)
	for i, v := range blobs {
		// C.GoBytes performs a copy of the underlying C memory.
		outSlice[i] = C.GoBytes(unsafe.Pointer(v.content), C.int(v.content_length))
	}

	return newReaderDataBlob(name, outSlice), nil
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

func (rd *ReaderDataString) ensureCapacity(n int) {
	if cap(rd.xs) < n {
		tmp := make([]string, len(rd.xs), n)
		copy(tmp, rd.xs)
		rd.xs = tmp
	}
}

func (rd *ReaderDataString) appendData(data ReaderData) error {
	if rd.name != data.Name() {
		return fmt.Errorf("name mismatch: expected '%s', got '%s'", rd.name, data.Name())
	}

	other, ok := data.(*ReaderDataString)
	if !ok {
		return fmt.Errorf("appendData: type mismatch, expected ReaderDataString, got %T", data)
	}

	rd.xs = append(rd.xs, other.xs...)
	return nil
}

func (rd *ReaderDataString) appendDataUnsafe(data ReaderData) {
	other := (*ReaderDataString)(ifaceDataPtr(data))
	rd.xs = append(rd.xs, other.xs...)
}

// Internal function used to convert C.qdb_exp_batch_push_column_t to Go. Memory-safe function
// that copies data.
//
// Assumes `data.data_type` is string, returns error otherwise.
//
// name: column name
// xs:   C array of reader column data
// n:    length of `data` inside array
func newReaderDataString(name string, xs []string) ReaderDataString {
	return ReaderDataString{name: name, xs: xs}
}

// newReaderDataStringFromNative converts the C representation of string columns to a
// Go-managed ReaderDataString.
func newReaderDataStringFromNative(name string, xs C.qdb_exp_batch_push_column_t, n int) (ReaderDataString, error) {
	if xs.data_type != C.qdb_ts_column_string {
		return ReaderDataString{}, fmt.Errorf("newReaderDataStringFromNative: expected data type string, got: %v", xs.data_type)
	}
	if n <= 0 {
		return ReaderDataString{}, fmt.Errorf("newReaderDataStringFromNative: invalid column length %d", n)
	}

	rawPtr := *(*unsafe.Pointer)(unsafe.Pointer(&xs.data[0]))
	if rawPtr == nil {
		return ReaderDataString{}, fmt.Errorf("newReaderDataStringFromNative: nil data pointer for column %s", name)
	}

	strs, err := cPointerArrayToSlice[C.qdb_string_t, C.qdb_string_t](rawPtr, int64(n))
	if err != nil {
		return ReaderDataString{}, fmt.Errorf("newReaderDataStringFromNative: %v", err)
	}

	goSlice := make([]string, n)
	for i, v := range strs {
		// C.GoStringN copies the C string content into Go-managed memory
		goSlice[i] = C.GoStringN(v.data, C.int(v.length))
	}

	return newReaderDataString(name, goSlice), nil
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

type ReaderChunk struct {
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
func (rt *ReaderChunk) TableName() string {
	return rt.tableName
}

// Returns number of rows in this chunk / table
func (rt *ReaderChunk) RowCount() string {
	return rt.tableName
}

// mergeReaderChunks merges multiple chunks of the same table into one unified chunk.
func mergeReaderChunks(xs []ReaderChunk) (ReaderChunk, error) {
	if len(xs) == 0 {
		return ReaderChunk{}, nil
	}

	base := xs[0]
	totalRows := len(base.idx)
	for i, chunk := range xs[1:] {
		if chunk.tableName != base.tableName {
			return ReaderChunk{}, fmt.Errorf("table name mismatch at position %d: expected '%s', got '%s'", i+1, base.tableName, chunk.tableName)
		}

		if len(chunk.data) != len(base.data) {
			return ReaderChunk{}, fmt.Errorf("column length mismatch at chunk %d: expected %d, got %d", i+1, len(base.data), len(chunk.data))
		}
		for ci, c := range chunk.data {
			if c.Name() != base.data[ci].Name() || c.valueType() != base.data[ci].valueType() {
				return ReaderChunk{}, fmt.Errorf("column mismatch at chunk %d, column %d: expected '%s(%v)', got '%s(%v)'", i+1, ci, base.data[ci].Name(), base.data[ci].valueType(), c.Name(), c.valueType())
			}
		}
		totalRows += len(chunk.idx)
	}

	mergedIdx := make([]time.Time, 0, totalRows)
	mergedData := make([]ReaderData, len(base.data))

	// Pre-allocate all data, useful when merging many smaller chunks into a larger chunk
	for idx := range len(base.data) {
		mergedData[idx].ensureCapacity(totalRows)
	}

	for _, chunk := range xs {
		mergedIdx = append(mergedIdx, chunk.idx...)
		for idx, col := range chunk.data {
			err := mergedData[idx].appendData(col)
			if err != nil {
				return ReaderChunk{}, fmt.Errorf("error appending data column %d: %w", idx, err)
			}
		}
	}

	return ReaderChunk{
		tableName:          base.tableName,
		idx:                mergedIdx,
		data:               mergedData,
		columnInfoByOffset: base.columnInfoByOffset}, nil
}

// Creates new ReaderChunk object out of a qdb_exp_batch_push_table_t struct. Memory-safe,
// in that it copies all the memory which means these objects are safe to use for a long time.
//
// As all schemas for all tables are required to be the same, this function accepts the `columns`
// parameter that were parsed earlier. For convenience, in our case, we set it as part of each
// ReaderChunk object.
func newReaderChunk(columns []ReaderColumn, tbl C.qdb_exp_batch_push_table_t) (ReaderChunk, error) {

	// Step 1: input validation
	if tbl.name == nil {
		return ReaderChunk{}, fmt.Errorf("Internal error: nil table name")
	}

	if tbl.data.timestamps == nil {
		return ReaderChunk{}, fmt.Errorf("Internal error: nil timestamps for table %s", C.GoString(tbl.name))
	}

	if tbl.data.columns == nil {
		return ReaderChunk{}, fmt.Errorf("Internal error: nil columns for table %s", C.GoString(tbl.name))
	}

	if tbl.data.row_count <= 0 {
		return ReaderChunk{}, fmt.Errorf("Internal error: invalid row count %d", tbl.data.row_count)
	}

	if tbl.data.column_count <= 0 {
		return ReaderChunk{}, fmt.Errorf("Internal error: invalid column count %d", tbl.data.column_count)
	}

	var out ReaderChunk
	out.columnInfoByOffset = columns

	// Copy table name from C memory
	out.tableName = C.GoString(tbl.name)

	// We mostly care about the actual data and will be using that struct a lot, so let's
	// acquire a reference to it.
	var data *C.qdb_exp_batch_push_table_data_t = &(tbl.data)

	// Set row count
	out.rowCount = int(data.row_count)

	// Copy index using utility to convert slice of C.qdb_timespec_t to []time.Time
	out.idx = QdbTimespecSliceToTime(unsafe.Slice(data.timestamps, out.rowCount))

	// Store the column data
	colCount := int(data.column_count)
	out.data = make([]ReaderData, colCount)

	columnSlice := unsafe.Slice(data.columns, colCount)
	for i := 0; i < colCount; i++ {
		column := columnSlice[i]

		expected := C.qdb_ts_column_type_t(columns[i].columnType)
		if column.data_type != expected {
			return ReaderChunk{}, fmt.Errorf("Internal error: column %d type mismatch (expected %v, got %v)", i, expected, column.data_type)
		}

		name := columns[i].columnName

		switch columns[i].columnType.AsValueType() {
		case TsValueInt64:
			v, err := newReaderDataInt64FromNative(name, column, out.rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueDouble:
			v, err := newReaderDataDoubleFromNative(name, column, out.rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueTimestamp:
			v, err := newReaderDataTimestampFromNative(name, column, out.rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueBlob:
			v, err := newReaderDataBlobFromNative(name, column, out.rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueString:
			v, err := newReaderDataStringFromNative(name, column, out.rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		default:
			return ReaderChunk{}, fmt.Errorf("Internal error: unsupported value type for column %s", name)
		}
	}

	return out, nil
}

type ReaderOptions struct {
	batchSize  int
	tables     []string
	columns    []string
	rangeStart time.Time
	rangeEnd   time.Time
}

// NewReaderOptions creates a new ReaderOptions value with default settings.
//
// ReaderOptions uses the builder pattern to make configuration flexible and readable.
// You typically create an options value with NewReaderOptions, and then chain one or more
// configuration methods like WithTables, WithColumns, WithBatchSize, and WithTimeRange.
//
// Example usage:
//
//	opts := NewReaderOptions().
//	    WithTables([]string{"table1", "table2"}).
//	    WithColumns([]string{"colA", "colB"}).
//	    WithBatchSize(10000).
//	    WithTimeRange(startTime, endTime)
//
// This options value can then be passed to NewReader to create a Reader
// instance:
//
//	reader, err := NewReader(handle, opts)
//
// See WithTables, WithColumns, WithBatchSize, and WithTimeRange for configuration details.
func NewReaderOptions() ReaderOptions {
	// Default to 32768 rows
	var defaultBatchSize int = 32 * 1024

	return ReaderOptions{batchSize: defaultBatchSize}
}

// Creates ReaderOptions object that fetches all data for all columns for a set of tables
func NewReaderDefaultOptions(tables []string) ReaderOptions {
	return NewReaderOptions().WithTables(tables)
}

func (ro ReaderOptions) WithBatchSize(batchSize int) ReaderOptions {
	ro.batchSize = batchSize
	return ro
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

// A batch is nothing more than a number of chunks
type ReaderBatch []ReaderChunk

// Merges multiple reader batches into one large batch.
func mergeReaderBatches(xs []ReaderBatch) (ReaderBatch, error) {
	// TODO: implement
	//
	// Step 1: short-circuit if there are either 0 or 1 batches

	if len(xs) == 0 {
		return nil, fmt.Errorf("No batches provided to mergeReaderBatches function")
	} else if len(xs) == 1 {
		return xs[0], nil
	}

	// Step 2: as all batches should have the exact same shape, we know exactly
	//         how many chunks we need to allocate by just looking at the first chunk.
	ret := make([]ReaderChunk, len(xs))

	// This will be our temporary "staging" area of all chunks we intend to merge together
	//
	// The number of different "chunk arrays" that we'll have to merge will be exactly identical
	// to the number of chunks of the first batch
	chunks := make([][]ReaderChunk, len(xs[0]))

	batchCount := len(xs)

	// Pre-allocate all the chunk slices
	for idx := range len(xs) {
		// Extra safety: if, for whatever reason, one of these batches has not the
		// *exact* same number of chunks as the first one, throw an internal error
		if len(xs[idx]) != len(xs[0]) {
			return nil, fmt.Errorf("Internal error: incorrect chunk count for batch at offset %d: %d != %d", idx, len(xs[idx]), len(xs[0]))
		}

		// We know that we will have exactly 1 chunk per batch that we want to merge
		chunks[idx] = make([]ReaderChunk, batchCount)
	}

	// Step 3: gather all "chunk arrays" into the correct places, basically doing a matrix transpose.
	//
	// If [batch][chunk] is the shape of xs:
	//
	//   batch 0:  chunk[0], chunk[1], ... chunk[N]
	//   batch 1:  chunk[0], chunk[1], ... chunk[N]
	//   batch 2:  chunk[0], chunk[1], ... chunk[N]
	//
	// the output becomes chunks[N][batch] (pivoted):
	//
	//   chunk[0]: batch[0], batch[1], batch[2], ...
	//   chunk[1]: batch[0], batch[1], batch[2], ...
	//   ...
	//
	// All memory in all chunks is pre-allocated we can just directly insert them:
	for chunkIdx := range chunks {
		for batchIdx := range xs {
			chunks[chunkIdx][batchIdx] = xs[batchIdx][chunkIdx]
		}
	}

	// Step 4: Merge each group of chunks (`chunkGroup`) into a single, consolidated chunk.
	//
	// Visually, right now we have organized chunks as follows after Step 3:
	//
	// chunks = [
	//   chunk 0: [batch 0 chunk 0, batch 1 chunk 0, ..., batch N chunk 0],
	//   chunk 1: [batch 0 chunk 1, batch 1 chunk 1, ..., batch N chunk 1],
	//   ...
	//   chunk M: [batch 0 chunk M, batch 1 chunk M, ..., batch N chunk M]
	// ]
	//
	// In this step, we'll reduce (merge) each chunk group horizontally:
	//
	// BEFORE MERGE:
	//
	//     batch 0          batch 1          batch N
	//    -------------------------------------------------
	// 0 | chunk[0,0]       chunk[1,0] ...   chunk[N,0]
	// 1 | chunk[0,1]       chunk[1,1] ...   chunk[N,1]
	// . |    ...              ...               ...
	// M | chunk[0,M]       chunk[1,M] ...   chunk[N,M]
	//
	//
	// AFTER MERGE:
	//
	// ----------------------------------------
	// ret[0] --> merge(chunk[0,0], chunk[1,0], ..., chunk[N,0])
	// ret[1] --> merge(chunk[0,1], chunk[1,1], ..., chunk[N,1])
	//     ...
	// ret[M] --> merge(chunk[0,M], chunk[1,M], ..., chunk[N,M])
	//
	for idx, chunkGroup := range chunks {
		// merge each horizontal group of chunks ("chunkGroup") into one single chunk
		mergedChunk, err := mergeReaderChunks(chunkGroup)
		if err != nil {
			return nil, fmt.Errorf("error merging chunks at index %d: %w", idx, err)
		}

		// assign into final slice
		ret[idx] = mergedChunk
	}

	return ret, nil
}

// Represents a Reader that enables traversing over all data.
type Reader struct {
	// Handle that was used to create the reader, and should be reused accross all additional
	// calls (specifically all memory allocations and/or qdb_release() invocations) in the scope
	// of this reader.
	handle HandleType

	// Options that were used to initialize the reader, for future reference if required
	options ReaderOptions

	// Current state of the reader handle.
	state C.qdb_reader_handle_t

	// Iterator pattern: keep track whether we previously ran into an error so the user can
	// look it up
	err error

	// Iterator pattern: true if we reached the end
	done bool

	// Iterator pattern: current batch we're pointing at
	currentBatch ReaderBatch
}

// NewReader returns a new Reader instance that allows bulk data retrieval from quasardb tables.
//
// Typical usage for retrieving data involves iteration like this:
//
//	reader := NewReader(options)
//	defer reader.Close()
//	for reader.Next() {
//	    batch := reader.Batch()
//	    // process batch
//	}
//	if err := reader.Err(); err != nil {
//	    // handle error appropriately
//	}
//
// To conveniently retrieve all available data as a single batch:
//
//	reader := NewReader(options)
//	defer reader.Close()
//	batch, err := reader.FetchAll()
//	if err != nil {
//	    // handle error appropriately
//	}
//	// process batch
//
// Always call reader.Close() to release any associated resources.
func NewReader(h HandleType, options ReaderOptions) (Reader, error) {
	var ret Reader
	ret.handle = h
	ret.options = options

	// Step 1: validations
	if len(options.tables) == 0 {
		return ret, fmt.Errorf("no tables provided")
	}

	// Either both rangeStart and rangeEnd must be zero (meaning no range
	// filtering) or both must be non-zero.  Having only one of them set is
	// invalid.
	if options.rangeStart.IsZero() != options.rangeEnd.IsZero() {
		return ret, fmt.Errorf("invalid time range")
	}

	if options.rangeEnd.IsZero() == false && !options.rangeEnd.After(options.rangeStart) {
		return ret, fmt.Errorf("invalid time range")
	}

	// Step 1: validate that our batchSize makes sense -- that it's not exceptionally large
	if options.batchSize <= 0 || options.batchSize > (1<<24) {
		return ret, fmt.Errorf("invalid batch size: %d", options.batchSize)
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
		ret.handle.handle,
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

// Fetches a single batch of data. Returns an array of 'ReaderChunk'
// objects
//
// Accepts the number of rows to get
func (r *Reader) fetchBatch() ([]ReaderChunk, error) {
	var cTables *C.qdb_bulk_reader_table_data_t
	tableCount := len(r.options.tables)
	err := makeErrorOrNil(C.qdb_bulk_reader_get_data(r.state, &cTables, C.qdb_size_t(r.options.batchSize)))

	if err == ErrIteratorEnd {
		if cTables != nil {
			qdbRelease(r.handle, cTables)
		}
		return []ReaderChunk{}, nil
	} else if err != nil {
		return nil, fmt.Errorf("unable to fetch bulk reader data: %v", err)
	} else if cTables == nil {
		return nil, fmt.Errorf("qdb_bulk_reader_get_data returned nil pointer")
	}

	defer releaseBulkReaderData(r.handle, cTables, tableCount)

	// Step 4: convert native structures to ReaderChunk objects
	tblSlice := unsafe.Slice(cTables, tableCount)

	ret := make([]ReaderChunk, tableCount)

	for i := range tableCount {
		tblData := tblSlice[i]

		colCount := int(tblData.column_count)
		colSlice := unsafe.Slice(tblData.columns, colCount)

		cols := make([]ReaderColumn, colCount)
		for j := range colCount {
			if colSlice[j].name == nil {
				return nil, fmt.Errorf("nil column name for table %s", r.options.tables[i])
			}
			cols[j] = ReaderColumn{
				columnName: C.GoString(colSlice[j].name),
				columnType: TsColumnType(colSlice[j].data_type),
			}
		}

		var tmp C.qdb_exp_batch_push_table_t
		cname := C.CString(r.options.tables[i])
		tmp.name = cname
		tmp.data = tblData

		rt, err := newReaderChunk(cols, tmp)
		C.free(unsafe.Pointer(cname))
		if err != nil {
			return nil, err
		}
		ret[i] = rt
	}

	return ret, nil
}

// releaseBulkReaderData releases C memory allocated by qdb_bulk_reader_get_data,
// including nested timestamp, column name and data arrays.
func releaseBulkReaderData(h HandleType, cTables *C.qdb_bulk_reader_table_data_t, tableCount int) {
	tblSlice := unsafe.Slice(cTables, tableCount)
	for i := range tableCount {
		data := tblSlice[i]
		if data.timestamps != nil {
			qdbRelease(h, data.timestamps)
			data.timestamps = nil
		}

		if data.columns != nil {
			colCount := int(data.column_count)
			colSlice := unsafe.Slice(data.columns, colCount)
			for j := range colCount {
				col := &colSlice[j]

				raw := *(*unsafe.Pointer)(unsafe.Pointer(&col.data[0]))
				switch col.data_type {
				case C.qdb_ts_column_int64:
					if raw != nil {
						qdbRelease(h, (*C.qdb_int_t)(raw))
					}
				case C.qdb_ts_column_double:
					if raw != nil {
						qdbRelease(h, (*C.double)(raw))
					}
				case C.qdb_ts_column_timestamp:
					if raw != nil {
						qdbRelease(h, (*C.qdb_timespec_t)(raw))
					}
				case C.qdb_ts_column_blob:
					if raw != nil {
						blobPtr := (*C.qdb_blob_t)(raw)
						blobSlice := unsafe.Slice(blobPtr, int(data.row_count))
						for k := range data.row_count {
							if blobSlice[k].content != nil {
								qdbReleasePointer(h, unsafe.Pointer(blobSlice[k].content))
								blobSlice[k].content = nil
							}
						}
						qdbRelease(h, blobPtr)
					}
				case C.qdb_ts_column_string:
					if raw != nil {
						strPtr := (*C.qdb_string_t)(raw)
						strSlice := unsafe.Slice(strPtr, int(data.row_count))
						for k := range data.row_count {
							if strSlice[k].data != nil {
								qdbReleasePointer(h, unsafe.Pointer(strSlice[k].data))
								strSlice[k].data = nil
							}
						}
						qdbRelease(h, strPtr)
					}
				}

				if col.name != nil {
					qdbRelease(h, colSlice[j].name)
					colSlice[j].name = nil
				}

			}

			qdbRelease(h, data.columns)
			data.columns = nil
		}

		qdbRelease(h, &data)

	}

	qdbRelease(h, cTables)
}

// Next prepares the next available data batch.
// It returns true if more data is available, false otherwise, making it suitable
// for idiomatic Go loops:
//
//	for reader.Next() { ... }
//
// Always use Err() afterward to check if iteration ended due to an error.
func (r *Reader) Next() bool {
	if r.done {
		return false
	}

	r.currentBatch, r.err = r.fetchBatch()

	if r.err != nil || len(r.currentBatch) == 0 {
		r.done = true
		return false
	}

	return true
}

// Err returns the error encountered during iteration, if any.
// After a completed iteration with Next(), Err() must be checked to
// distinguish between successful completion and errors during reading.
func (r *Reader) Err() error {
	return r.err
}

// Batch returns the current batch of data prepared by Next().
// It's designed to be called within a Next() loop:
//
//	for reader.Next() {
//	    batch := reader.Batch()
//	    // use batch
//	}
func (r *Reader) Batch() ReaderBatch {
	return r.currentBatch
}

// FetchAll retrieves the entire dataset as a single batch to simplify interaction when the entire result set is required.
//
// It internally manages batching and merging of data, eliminating the need for callers to iterate manually.
//
// Important: Consider memory usage carefully — FetchAll may consume significant resources when reading large datasets.
// Use this function when convenience outweighs potential memory overhead.
func (r *Reader) FetchAll() (ReaderBatch, error) {
	// Accumulate all batches from the reader's iterator, and merges them together in a single
	// batch.

	// Allocate batches with an initial capacity to avoid frequent reallocations
	// because we expect potentially many batches for large reads.
	batches := make([]ReaderBatch, 0, 256)

	// Iterate until we've collected all available data batches
	for r.Next() {
		batch := r.Batch()
		batches = append(batches, batch)
	}

	if err := r.Err(); err != nil {
		return nil, err
	}

	finalBatch, err := mergeReaderBatches(batches)
	if err != nil {
		return nil, err
	}

	return finalBatch, nil
}

// Releases underlying memory
func (r *Reader) Close() {
	// if state is non-nil, invoke qdbRelease() on state
	if r.state != nil {

		qdbReleasePointer(r.handle, unsafe.Pointer(r.state))

		r.state = nil
	}

}
