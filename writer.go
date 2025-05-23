// Package qdb provides an api to a quasardb server
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"
)

type writerError struct {
	s string
}

func (e *writerError) Error() string {
	return e.s
}

func New(text string) error {
	return &writerError{text}
}

type WriterData interface {
	// Possibly some methods, but often empty
	valueType() TsValueType
}

// Int64
type WriterDataInt64 struct {
	Values []int64
}

func (wd WriterDataInt64) valueType() TsValueType {
	return TsValueInt64
}

// Double
type WriterDataDouble struct {
	Values []float64
}

func (cd WriterDataDouble) valueType() TsValueType {
	return TsValueDouble
}

// Timestamp
type WriterDataTimestamp struct {
	Values []C.qdb_timespec_t
}

func (cd WriterDataTimestamp) valueType() TsValueType {
	return TsValueTimestamp
}

// Blob
type WriterDataBlob struct {
	Values [][]byte
}

func (cd WriterDataBlob) valueType() TsValueType {
	return TsValueBlob
}

// String
type WriterDataString struct {
	Values []string
}

func (cd WriterDataString) valueType() TsValueType {
	return TsValueString
}

// Metadata we need to represent a single column.
type WriterColumn struct {
	ColumnName string
	ColumnType TsColumnType
}

// Single table to be provided to the batch writer.
type WriterTable struct {
	TableName string

	// All arrays are guaranteed to be of size `len`
	rowCount int

	// An index that enables looking up of a column's name by its offset within the table.
	columnInfoByOffset []WriterColumn

	// An index that enables looking up of a column's offset within the table by its name.
	columnOffsetByName map[string]int

	// The index, can not contain null values
	idx []C.qdb_timespec_t

	// Value arrays to write for each column.
	data []WriterData
}

type WriterPushMode C.qdb_exp_batch_push_mode_t

const (
	WriterPushModeTransactional WriterPushMode = C.qdb_exp_batch_push_transactional
	WriterPushModeFast          WriterPushMode = C.qdb_exp_batch_push_fast
	WriterPushModeAsync         WriterPushMode = C.qdb_exp_batch_push_async
)

type WriterOptions struct {
	pushMode             WriterPushMode
	dropDuplicates       bool
	dropDuplicateColumns []string
}

type Writer struct {
	options WriterOptions
	tables  map[string]WriterTable
}

// Constructor for in64 data array
func NewWriterDataInt64(xs []int64) WriterData {
	return WriterDataInt64{Values: xs}
}

// Constructor for double data array
func NewWriterDataDouble(xs []float64) WriterData {
	return WriterDataDouble{Values: xs}
}

// Constructor for timestamp data array
func NewWriterDataTimestamp(xs []C.qdb_timespec_t) WriterData {
	return WriterDataTimestamp{Values: xs}
}

// Constructor for timestamp data array
func NewWriterDataTimestampFromTimeSlice(xs []time.Time) WriterData {
	return NewWriterDataTimestamp(TimeSliceToQdbTimespec(xs))
}

// Constructor for blob data array
func NewWriterDataBlob(xs [][]byte) WriterData {
	return WriterDataBlob{Values: xs}
}

// Constructor for string data array
func NewWriterDataString(xs []string) WriterData {
	return WriterDataString{Values: xs}
}

func ifaceDataPtr(i interface{}) unsafe.Pointer {
	// internal helper in your package (private, defined once)
	type iface struct {
		tab  unsafe.Pointer
		data unsafe.Pointer
	}

	return (*iface)(unsafe.Pointer(&i)).data
}

// GetInt64Array safely converts WriterData to *WriterDataInt64.
//
// Returns an error if data is not of type Int64.
func GetInt64Array(x WriterData) (*WriterDataInt64, error) {
	v, ok := x.(WriterDataInt64)
	if !ok {
		return nil, fmt.Errorf("GetInt64Array: type mismatch, expected WriterDataInt64, got %T", x)
	}
	return &v, nil
}

// GetInt64ArrayUnsafe is an unsafe version of GetInt64Array. Undefined behavior occurs when
// invoked on the incorrect type.
func GetInt64ArrayUnsafe(x WriterData) *WriterDataInt64 {
	return (*WriterDataInt64)(ifaceDataPtr(x))
}

// GetDoubleArray safely converts WriterData to *WriterDataDouble.
//
// Returns an error if data is not of type Double.
func GetDoubleArray(x WriterData) (*WriterDataDouble, error) {
	v, ok := x.(WriterDataDouble)
	if !ok {
		return nil, fmt.Errorf("GetDoubleArray: type mismatch, expected WriterDataDouble, got %T", x)
	}
	return &v, nil
}

// GetDoubleArrayUnsafe is an unsafe version of GetDoubleArray. Undefined behavior occurs when
// invoked on the incorrect type.
func GetDoubleArrayUnsafe(x WriterData) *WriterDataDouble {
	return (*WriterDataDouble)(ifaceDataPtr(x))
}

// GetTimestampArray safely converts WriterData to *WriterDataTimestamp.
//
// Returns an error if data is not of type Timestamp.
func GetTimestampArray(x WriterData) (*WriterDataTimestamp, error) {
	v, ok := x.(WriterDataTimestamp)
	if !ok {
		return nil, fmt.Errorf("GetTimestampArray: type mismatch, expected WriterDataTimestamp, got %T", x)
	}
	return &v, nil
}

// GetTimestampArrayUnsafe is an unsafe version of GetTimestampArray. Undefined behavior occurs when
// invoked on the incorrect type.
func GetTimestampArrayUnsafe(x WriterData) *WriterDataTimestamp {
	return (*WriterDataTimestamp)(ifaceDataPtr(x))
}

// GetStringArray safely converts WriterData to *WriterDataString.
//
// Returns an error if data is not of type String.
func GetStringArray(x WriterData) (*WriterDataString, error) {
	v, ok := x.(WriterDataString)
	if !ok {
		return nil, fmt.Errorf("GetStringArray: type mismatch, expected WriterDataString, got %T", x)
	}
	return &v, nil
}

// GetStringArrayUnsafe is an unsafe version of GetStringArray. Undefined behavior occurs when
// invoked on the incorrect type.
func GetStringArrayUnsafe(x WriterData) *WriterDataString {
	return (*WriterDataString)(ifaceDataPtr(x))
}

// GetBlobArray safely converts WriterData to *WriterDataBlob.
//
// Returns an error if data is not of type Blob.
func GetBlobArray(x WriterData) (*WriterDataBlob, error) {
	v, ok := x.(WriterDataBlob)
	if !ok {
		return nil, fmt.Errorf("GetBlobArray: type mismatch, expected WriterDataBlob, got %T", x)
	}
	return &v, nil
}

// GetBlobArrayUnsafe is an unsafe version of GetBlobArray. Undefined behavior occurs when
// invoked on the incorrect type.
func GetBlobArrayUnsafe(x WriterData) *WriterDataBlob {
	return (*WriterDataBlob)(ifaceDataPtr(x))
}

func NewWriterTable(t string, cols []WriterColumn) WriterTable {
	// Pre-allocate our data array, which has exactly 1 entry for every column we intend to write.
	data := make([]WriterData, len(cols))

	// Build indexes
	columnInfoByOffset := make([]WriterColumn, len(cols))
	columnOffsetByName := make(map[string]int)

	for i, col := range cols {
		columnInfoByOffset[i] = col
		columnOffsetByName[col.ColumnName] = i
	}

	// An index of column offset to name
	return WriterTable{t, -1, columnInfoByOffset, columnOffsetByName, nil, data}
}

func (t WriterTable) GetName() string {
	return t.TableName
}

// Batch writer. Accepts options and data

func (t *WriterTable) SetIndex(idx []C.qdb_timespec_t) error {
	t.idx = idx
	t.rowCount = len(idx)

	return nil
}

func (t WriterTable) GetIndex() []C.qdb_timespec_t {
	return t.idx
}

// Sets data for a single column
func (t *WriterTable) SetData(offset int, xs WriterData) error {
	if len(t.columnInfoByOffset) <= offset {
		return fmt.Errorf("Column offset out of range: %v", offset)
	}

	col := t.columnInfoByOffset[offset]
	if col.ColumnType.AsValueType() != xs.valueType() {
		return fmt.Errorf("Column's expected value type does not match provided value type: column type (%v)'s value type %v != %v", col.ColumnType, col.ColumnType.AsValueType(), xs.valueType())
	}

	t.data[offset] = xs

	return nil
}

// Sets all all column data for a single table into the writer, assumes offsets of provided
// data are aligned with the table.
func (t *WriterTable) SetDatas(xs []WriterData) error {
	for i, x := range xs {
		err := t.SetData(i, x)

		if err != nil {
			return err
		}
	}

	return nil
}

func (t WriterTable) GetData(offset int) (WriterData, error) {
	if offset >= len(t.data) {
		return nil, fmt.Errorf("Column offset out of range: %v", offset)
	}

	return t.data[offset], nil
}

// Returns new WriterOptions struct with default options set.
func NewWriterOptions() WriterOptions {
	return WriterOptions{WriterPushModeTransactional, false, []string{}}
}

// Returns the currently set push mode
func (options WriterOptions) GetPushMode() WriterPushMode {
	return options.pushMode
}

// Returns true if deduplication is enabled
func (options WriterOptions) IsDropDuplicatesEnabled() bool {
	return options.dropDuplicates
}

// Enables deduplication based on all columns
func (options WriterOptions) EnableDropDuplicates() WriterOptions {
	options.dropDuplicates = true
	return options
}

// Enables deduplicates based on the provided columns
func (options WriterOptions) EnableDropDuplicatesOn(columns []string) WriterOptions {
	options.dropDuplicates = true
	options.dropDuplicateColumns = columns
	return options
}

// Returns the columns to be deduplicated on. If empty, deduplicates based on
// equality of all columns.
func (options WriterOptions) GetDropDuplicateColumns() []string {
	return options.dropDuplicateColumns
}

// Sets the push mode to the desired push mode
func (options WriterOptions) WithPushMode(mode WriterPushMode) WriterOptions {
	options.pushMode = mode
	return options
}

// Shortcut for `WithPushMode(WriterPushModeAsync)`
func (options WriterOptions) WithAsyncPush() WriterOptions {
	return options.WithPushMode(WriterPushModeAsync)
}

// Shortcut for `WithPushMode(WriterPushModeAsync)`
func (options WriterOptions) WithFastPush() WriterOptions {
	return options.WithPushMode(WriterPushModeFast)
}

// Shortcut for `WithPushMode(WriterPushModeAsync)`
func (options WriterOptions) WithTransactionalPush() WriterOptions {
	return options.WithPushMode(WriterPushModeTransactional)
}

// Creates a new Writer with the provided options
func NewWriter(options WriterOptions) Writer {
	return Writer{options: options, tables: make(map[string]WriterTable)}
}

// Creates a new Writer with default options
func NewWriterWithDefaultOptions() Writer {
	return NewWriter(NewWriterOptions())
}

// Returns the writer's options
func (w Writer) GetOptions() WriterOptions {
	return w.options
}

// Sets the data of a table. Returns error if table already exists.
func (w *Writer) SetTable(t WriterTable) error {
	// Check if the table already exists
	_, exists := w.tables[t.TableName]
	if exists {
		return fmt.Errorf("table %q already exists", t.TableName)
	}

	w.tables[t.TableName] = t

	return nil
}

// Returns the table with the provided name
func (w *Writer) GetTable(name string) (WriterTable, error) {
	t, ok := w.tables[name]
	if !ok {
		return WriterTable{}, fmt.Errorf("Table not found: %s", name)
	}

	return t, nil
}

// Returns the number of tables the writer currently holds.
func (w *Writer) Length() int {
	return len(w.tables)
}

// Pushes all tables to the server according to PushOptions.
func (w *Writer) Push() error {
	if w.Length() == 0 {
		return fmt.Errorf("No tables to push")
	}

	return nil
}
