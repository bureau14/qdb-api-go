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
	EnsureCapacity(n int)

	// Ensures underlying data is reset to 0
	Clear()

	// Returns the number of items in the array
	Length() int

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

func (rd *ReaderDataInt64) Length() int {
	return len(rd.xs)
}

func (rd *ReaderDataInt64) valueType() TsValueType {
	return TsValueInt64
}

func (rd *ReaderDataInt64) EnsureCapacity(n int) {
	rd.xs = sliceEnsureCapacity(rd.xs, n)
}

func (rd *ReaderDataInt64) Clear() {
	rd.xs = make([]int64, 0)
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

func (rd *ReaderDataDouble) Length() int {
	return len(rd.xs)
}

func (rd *ReaderDataDouble) valueType() TsValueType {
	return TsValueDouble
}

func (rd *ReaderDataDouble) EnsureCapacity(n int) {
	rd.xs = sliceEnsureCapacity(rd.xs, n)
}

func (rd *ReaderDataDouble) Clear() {
	rd.xs = make([]float64, 0)
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

func (rd *ReaderDataTimestamp) Length() int {
	return len(rd.xs)
}

func (rd *ReaderDataTimestamp) valueType() TsValueType {
	return TsValueTimestamp
}

func (rd *ReaderDataTimestamp) EnsureCapacity(n int) {
	rd.xs = sliceEnsureCapacity(rd.xs, n)
}

func (rd *ReaderDataTimestamp) Clear() {
	rd.xs = make([]time.Time, 0)
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

func (rd *ReaderDataBlob) Length() int {
	return len(rd.xs)
}

func (rd *ReaderDataBlob) valueType() TsValueType {
	return TsValueBlob
}

func (rd *ReaderDataBlob) EnsureCapacity(n int) {
	rd.xs = sliceEnsureCapacity(rd.xs, n)
}

func (rd *ReaderDataBlob) Clear() {
	rd.xs = make([][]byte, 0)
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

func (rd *ReaderDataString) Length() int {
	return len(rd.xs)
}

func (rd *ReaderDataString) valueType() TsValueType {
	return TsValueString
}

func (rd *ReaderDataString) EnsureCapacity(n int) {
	rd.xs = sliceEnsureCapacity(rd.xs, n)
}

func (rd *ReaderDataString) Clear() {
	rd.xs = make([]string, 0)
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

// ReaderColumn constructor
func NewReaderColumn(n string, t TsColumnType) (ReaderColumn, error) {
	if t.IsValid() == false {
		return ReaderColumn{}, fmt.Errorf("NewReaderColumn: invalid column: %v", t)
	}
	return ReaderColumn{columnName: n, columnType: t}, nil
}

// ReaderColumn constructor from native types.
func NewReaderColumnFromNative(n *C.char, t C.qdb_ts_column_type_t) (ReaderColumn, error) {
	if n == nil {
		return ReaderColumn{}, fmt.Errorf("NewReaderColumnFromNative: got null string reference for column name: %v", n)
	}

	return ReaderColumn{
		columnName: C.GoString(n),
		columnType: TsColumnType(t),
	}, nil
}

func (rc ReaderColumn) Name() string {
	return rc.columnName
}

func (rc ReaderColumn) Type() TsColumnType {
	return rc.columnType
}

type ReaderChunk struct {
	// An index that enables looking up of a column's name by its offset within the table.
	columnInfoByOffset []ReaderColumn

	// The index, can not contain null values
	idx []time.Time

	// Value arrays read from each column
	data []ReaderData
}

// NewReaderChunk constructs a ReaderChunk from explicit column metadata,
// index values and column data slices.
//
// Decision rationale:
//   - Separates validation from data copying, enabling reuse in tests and
//     production code.
//
// Key assumptions:
//   - len(cols) == len(data).
//   - len(idx) matches data[i].Length() for each column.
//   - Callers validate these preconditions; TODO above indicates missing checks.
func NewReaderChunk(cols []ReaderColumn, idx []time.Time, data []ReaderData) (ReaderChunk, error) {
	return ReaderChunk{
		columnInfoByOffset: cols,
		idx:                idx,
		data:               data,
	}, nil
}

func (rc *ReaderChunk) Empty() bool {
	// Returns true if no data
	return rc.idx == nil || len(rc.idx) == 0 || rc.data == nil || len(rc.data) == 0
}

// Clears all column data and index
func (rc *ReaderChunk) Clear() {
	// Empty slice
	rc.idx = make([]time.Time, 0)

	for i := range len(rc.data) {
		rc.data[i].Clear()
	}
}

// Ensures index and all data arrays have a certain capacity
func (rc *ReaderChunk) EnsureCapacity(n int) {
	rc.idx = sliceEnsureCapacity(rc.idx, n)

	for i := range len(rc.data) {
		// ReaderData has its own virtual method to ensure capacity,
		// as they're all of different types
		rc.data[i].EnsureCapacity(n)
	}
}

// Returns number of rows in this chunk / table
func (rc *ReaderChunk) RowCount() int {
	return len(rc.idx)
}

// mergeReaderChunks merges multiple chunks of the same table into one unified chunk.
//
// Decision rationale:
//   - Consolidates batch iteration results into a single structure for simpler
//     comparison and verification.
//
// Key assumptions:
//   - xs is non-empty and all chunks share identical schemas.
//
// Performance trade-offs:
//   - Requires copying all row data once; acceptable for test sizes.
func mergeReaderChunks(xs []ReaderChunk) (ReaderChunk, error) {
	if len(xs) == 0 {
		return ReaderChunk{}, nil
	}

	var base ReaderChunk = xs[0]
	var totalRows int = 0

	// Short-circuit in case there is just a single chunk, which is actuallyu a common case
	if len(xs) == 1 {
		return base, nil
	}

	for i, chunk := range xs[1:] {
		if len(chunk.data) != len(base.data) {
			return base, fmt.Errorf("column length mismatch at chunk %d: expected %d, got %d", i+1, len(base.data), len(chunk.data))
		}
		for ci, c := range chunk.data {
			if c.Name() != base.data[ci].Name() || c.valueType() != base.data[ci].valueType() {
				return base, fmt.Errorf("column mismatch at chunk %d, column %d: expected '%s(%v)', got '%s(%v)'", i+1, ci, base.data[ci].Name(), base.data[ci].valueType(), c.Name(), c.valueType())
			}
		}
		totalRows += len(chunk.idx)
	}

	mergedIdx := make([]time.Time, 0, totalRows)
	mergedData := make([]ReaderData, len(base.data))

	// Pre-allocate all data, useful when merging many smaller chunks into a larger chunk
	for idx, col := range base.data {
		// Rather than a lot of boilerplate, we just reuse the input object of the
		// base object, and reset that object's content to 0.
		//
		// This keeps the code small.
		//
		// We do need to make sure that we actually get "rid" of the references of
		// the old column, as slices are typically passed by reference, so all cols
		// would be pointing to the same slice reference
		var newCol ReaderData = col

		// Resets the actual held data, but not the column data / name
		newCol.Clear()

		// Ensure that the slice backing array can hold the final merged size
		newCol.EnsureCapacity(totalRows)

		mergedData[idx] = newCol
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
		idx:                mergedIdx,
		data:               mergedData,
		columnInfoByOffset: base.columnInfoByOffset}, nil
}

// newReaderChunk converts the native C table representation into a Go ReaderChunk.
//
// Decision rationale:
//   - Encapsulates the logic for translating C types and copying data into
//     Go-managed memory.
//
// Key assumptions:
//   - `columns` describes the schema encoded in `data`.
//   - `data` originates from qdb_exp_batch_push_table_data_t with valid pointers
//     and counts.
//
// Performance trade-offs:
//   - Copies all row data into Go slices for safety; incurs O(n) allocation but
//     ensures lifetime independence from the C buffer.
func newReaderChunk(columns []ReaderColumn, data C.qdb_exp_batch_push_table_data_t) (ReaderChunk, error) {

	if data.timestamps == nil {
		return ReaderChunk{}, fmt.Errorf("Internal error: nil timestamps")
	}

	if data.columns == nil {
		return ReaderChunk{}, fmt.Errorf("Internal error: nil columns")
	}

	if data.row_count <= 0 {
		return ReaderChunk{}, fmt.Errorf("Internal error: invalid row count %d", data.row_count)
	}

	if data.column_count <= 0 {
		return ReaderChunk{}, fmt.Errorf("Internal error: invalid column count %d", data.column_count)
	}

	var out ReaderChunk
	out.columnInfoByOffset = columns

	var rowCount int = int(data.row_count)

	// Copy index using utility to convert slice of C.qdb_timespec_t to []time.Time
	out.idx = QdbTimespecSliceToTime(unsafe.Slice(data.timestamps, rowCount))

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
			v, err := newReaderDataInt64FromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueDouble:
			v, err := newReaderDataDoubleFromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueTimestamp:
			v, err := newReaderDataTimestampFromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueBlob:
			v, err := newReaderDataBlobFromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueString:
			v, err := newReaderDataStringFromNative(name, column, rowCount)
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
	currentBatch ReaderChunk
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

	// Only ever a single range, so we can stack-allocate it and share directly with
	// the C API invocation.
	var cRanges [1]C.qdb_ts_range_t
	var cRangeCount C.qdb_size_t = 0

	// Important: we pass a null-pointer to the C API in case no ranges are provided.
	//
	// By relying on the functionality of qdb_bulk reader's C implementation that null
	// ranges implies "forever", we avoid the issue of having to find a way to
	// "represent" a forever range
	var cRangePtr *C.qdb_ts_range_t = nil

	// But if a range is provided, set the pointer accordingly.
	if options.rangeStart.IsZero() == false && options.rangeEnd.IsZero() == false {
		cRanges[0] = toQdbRange(options.rangeStart, options.rangeEnd)

		// Points to stack-allocated value
		cRangePtr = &cRanges[0]
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
		tblSlice[i].ranges = cRangePtr
		tblSlice[i].range_count = cRangeCount
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

	err = makeErrorOrNil(errCode)

	if err != nil {
		return ret, err
	}

	ret.state = readerHandle

	// Done, return state.

	return ret, nil
}

// fetchBatch retrieves up to r.options.batchSize rows from the server.
//
// Decision rationale:
//   - Wraps the qdb_bulk_reader_get_data call and converts the native format
//     to a Go ReaderChunk.
//
// Key assumptions:
//   - r.state is a valid reader handle.
//   - Caller handles ErrIteratorEnd appropriately.
//
// Performance trade-offs:
//   - Allocates new slices for column data each call; acceptable for test-sized
//     batches but avoid for extremely large data sets.
//
// Usage example:
//
//	batch, err := r.fetchBatch()
//	if err != nil { /* handle */ }
func (r *Reader) fetchBatch() (ReaderChunk, error) {
	var ret ReaderChunk
	var ptr *C.qdb_bulk_reader_table_data_t

	// qdb_bulk_reader_get_data "fills" a pointer in style of when you would get data back
	// for a number of tables, but it returns just a pointer for a single table. all memory
	// allocated within this function call is linked to this single object, and a qdbRelease
	// clears eerything
	errCode := C.qdb_bulk_reader_get_data(r.state, &ptr, C.qdb_size_t(r.options.batchSize))
	err := makeErrorOrNil(errCode)

	// Trigger the `defer` statement as there are failure scenarios in both cases where err
	// is nil or not-nil. That's why we put the ptr-check before checking error return codes.
	if ptr != nil {
		// All data in the `ptr`is allocated on the QuasarDB C-API side,
		// and as such is linked to this one "root" pointer.
		//
		// By invoking qdbRelease on it, it automatically recursively releases
		// all attached objects.
		defer qdbRelease(r.handle, ptr)
	}

	if err == ErrIteratorEnd {
		if ptr != nil {
			return ret, fmt.Errorf("fetchBatch iterator end, did not expect table data: %v\n", ptr)
		}

		// Return empty batch
		return ret, nil
	} else if err != nil {
		return ret, fmt.Errorf("unable to fetch bulk reader data: %v", err)
	} else if ptr == nil {
		return ret, fmt.Errorf("qdb_bulk_reader_get_data returned nil pointer")
	}

	table := *ptr

	if table.columns == nil || table.column_count <= 0 {
		return ret, fmt.Errorf("invalid column metadata (columns=%v column_count=%d)", table.columns, table.column_count)
	}

	colCount := int(table.column_count)
	colSlice := unsafe.Slice(table.columns, colCount)

	cols := make([]ReaderColumn, colCount)
	for j := range colCount {
		cols[j], err = NewReaderColumnFromNative(colSlice[j].name, colSlice[j].data_type)

		if err != nil {
			return ret, err
		}
	}

	ret, err = newReaderChunk(cols, table)
	if err != nil {
		return ret, err
	}

	return ret, nil
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

	if r.err != nil || r.currentBatch.Empty() == true {
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
func (r *Reader) Batch() ReaderChunk {
	return r.currentBatch
}

// FetchAll retrieves the entire dataset as a single batch to simplify interaction when the entire result set is required.
//
// It internally manages batching and merging of data, eliminating the need for callers to iterate manually.
//
// Important: Consider memory usage carefully — FetchAll may consume significant resources when reading large datasets.
// Use this function when convenience outweighs potential memory overhead.
func (r *Reader) FetchAll() (ReaderChunk, error) {
	// Accumulate all batches from the reader's iterator, and merges them together in a single
	// batch.

	var ret ReaderChunk
	var err error

	// Allocate batches with an initial capacity to avoid frequent reallocations
	// because we expect potentially many batches for large reads.
	batches := make([]ReaderChunk, 0, 4)

	// Iterate until we've collected all available data batches
	for r.Next() {
		batch := r.Batch()
		batches = append(batches, batch)
	}

	if r.Err() != nil {
		return ret, fmt.Errorf("Reader.FetchAll: error while traversing data: %v\n", r.Err())
	}

	ret, err = mergeReaderChunks(batches)
	if err != nil {
		return ret, err
	}

	return ret, nil
}

// Releases underlying memory
func (r *Reader) Close() {
	// if state is non-nil, invoke qdbRelease() on state
	if r.state != nil {

		qdbReleasePointer(r.handle, unsafe.Pointer(r.state))

		r.state = nil
	}

}
