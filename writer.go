// Package qdb provides an api to a quasardb server
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

import (
	"errors"
	"fmt"
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

// A single column's data within a WriterTable, representing values to be inserted as a single column into QuasarDB.
// These objects are relatively cheap to copy, as all internal arrays are pointers.
type WriterData struct {
	ValueType TsValueType

	// int64 value array. Only set if ValueType is Int64
	Int64Array *[]int64

	// double value array. Only set if ValueType is Double
	DoubleArray *[]float64

	// timestamp value array. Only set if ValueType is Timestamp
	TimestampArray *[]C.qdb_timespec_t

	// blob value array. Only set if ValueType is Blob
	BlobArray *[][]byte

	// string value array. Only set if ValueType is String
	StringArray *[]string
}

// Metadata we need to represent a single column.
type WriterColumn struct {
	ColumnName string
	ColumnType TsColumnType
}

// Single table to be provided to the batch writer
type WriterTable struct {
	TableName string

	// All arrays are guaranteed to be of size `len`
	rowCount int

	// An index that enables looking up of a column's name by its offset within the table.
	columnInfoByOffset []WriterColumn

	// An index that enables looking up of a column's offset within the table by its name.
	columnOffsetByName map[string]int

	// The index, can not contain null values
	idx *[]C.qdb_timespec_t

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
func NewWriterDataInt64(xs *[]int64) WriterData {
	return WriterData{ValueType: TsValueInt64, Int64Array: xs}
}

// Constructor for double data array
func NewWriterDataDouble(xs *[]float64) WriterData {
	return WriterData{ValueType: TsValueDouble, DoubleArray: xs}
}

// Constructor for timestamp data array
func NewWriterDataTimestamp(xs *[]C.qdb_timespec_t) WriterData {
	return WriterData{ValueType: TsValueTimestamp, TimestampArray: xs}
}

// Constructor for blob data array
func NewWriterDataBlob(xs *[][]byte) WriterData {
	return WriterData{ValueType: TsValueBlob, BlobArray: xs}
}

// Constructor for string data array
func NewWriterDataString(xs *[]string) WriterData {
	return WriterData{ValueType: TsValueString, StringArray: xs}
}

func (t WriterData) GetInt64Array() (*[]int64, error) {
	// This would be an internal error
	if t.ValueType != TsValueInt64 {
		return nil, errors.New(fmt.Sprintf("Not an int64 column type: %v", t.ValueType))
	}

	return t.Int64Array, nil
}

func (t WriterData) GetDoubleArray() (*[]float64, error) {
	// This would be an internal error
	if t.ValueType != TsValueDouble {
		return nil, errors.New(fmt.Sprintf("Not a float64  column type: %v", t.ValueType))
	}

	return t.DoubleArray, nil
}

func NewWriterTable(t string, cols []WriterColumn) *WriterTable {
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
	return &WriterTable{t, -1, columnInfoByOffset, columnOffsetByName, nil, data}
}

func (t WriterTable) GetName() string {
	return t.TableName
}

// Batch writer. Accepts options and data

func (t *WriterTable) SetIndex(idx *[]C.qdb_timespec_t) error {
	t.idx = idx
	t.rowCount = len(*idx)

	return nil
}

func (t WriterTable) GetIndex() *[]C.qdb_timespec_t {
	return t.idx
}

// Sets data for a single column
func (t *WriterTable) SetData(offset int, xs WriterData) error {
	if len(t.columnInfoByOffset) <= offset {
		return errors.New(fmt.Sprintf("Column offset out of range: %v", offset))
	}

	col := t.columnInfoByOffset[offset]
	if col.ColumnType.AsValueType() != xs.ValueType {
		return errors.New(fmt.Sprintf("Column's expected value type does not match provided value type: column type (%v)'s value type %v != %v", col.ColumnType, col.ColumnType.AsValueType(), xs.ValueType))
	}

	t.data[offset] = xs

	return nil
}

func (t WriterTable) GetData(offset int) (WriterData, error) {
	if offset >= len(t.data) {
		return WriterData{}, errors.New(fmt.Sprintf("Column offset out of range: %v", offset))
	}

	return t.data[offset], nil
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
