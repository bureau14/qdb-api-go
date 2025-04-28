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

// A single slice of data within a WriterTable, representing values to be inserted as a single column into QuasarDB
type WriterData struct {
	ValueType TsValueType

	// int64 value array. Only set if ValueType is Int64
	Int64Array []int64

	// double value array. Only set if ValueType is Double
	DoubleArray []float64

	// timestamp value array. Only set if ValueType is Timestamp
	TimestampArray []C.qdb_timespec_t

	// blob value array. Only set if ValueType is Blob
	BlobArray [][]byte

	// string value array. Only set if ValueType is String
	StringArray []string
}

// Constructor for in64 data array
func NewWriterDataInt64(xs []int64) WriterData {
	return WriterData{ValueType: TsValueInt64, Int64Array: xs}
}

// Constructor for double data array
func NewWriterDataDouble(xs []float64) WriterData {
	return WriterData{ValueType: TsValueDouble, DoubleArray: xs}
}

// Constructor for timestamp data array
func NewWriterDataTimestamp(xs []C.qdb_timespec_t) WriterData {
	return WriterData{ValueType: TsValueTimestamp, TimestampArray: xs}
}

// Constructor for blob data array
func NewWriterDataBlob(xs [][]byte) WriterData {
	return WriterData{ValueType: TsValueBlob, BlobArray: xs}
}

// Constructor for string data array
func NewWriterDataString(xs []string) WriterData {
	return WriterData{ValueType: TsValueString, StringArray: xs}
}

func (t WriterData) GetInt64Array() ([]int64, error) {
	// This would be an internal error
	if t.ValueType != TsValueString {
		return nil, errors.New(fmt.Sprintf("Not an int64 column type: %v", t.ValueType))
	}

	return t.Int64Array, nil
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
	idx []C.qdb_timespec_t

	// Value arrays to write for each column.
	data []WriterData
}

func NewWriterTable(t string, cols []WriterColumn) (*WriterTable, error) {
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
	return &WriterTable{t, -1, columnInfoByOffset, columnOffsetByName, nil, data}, nil
}

func (t WriterTable) SetIndex(idx []C.qdb_timespec_t) error {
	t.idx = idx
	t.rowCount = len(idx)

	return nil
}

func (t WriterTable) GetIndex() []C.qdb_timespec_t {
	return t.idx
}

func (t WriterTable) SetData(offset int, xs WriterData) error {
	t.data[offset] = xs

	return nil
}
