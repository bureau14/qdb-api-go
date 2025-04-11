// Package qdb provides an api to a quasardb server
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

// Single table to be provided to the batch writer
type WriterTable struct {

	TableName string;

	// All arrays are guaranteed to be of size `len`
	row_len int

	idx []C.qdb_timespec_t
}

func NewWriterTable(t string, cols []string) (*WriterTable, error) {
	return &WriterTable{t, -1, nil}, nil
}


func (t WriterTable) SetIndex(idx []C.qdb_timespec_t) error {
	t.idx = idx

	return nil
}
