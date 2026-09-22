// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: QuasarDB Go client API
// Arrow output for the bulk Reader.
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>

	#cgo noescape qdb_bulk_reader_get_data_arrow
	#cgo nocallback qdb_bulk_reader_get_data_arrow
*/
import "C"

import (
	"errors"
	"iter"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// arrowStream is one qdb_bulk_reader_get_data_arrow result: the record
// reader that owns the moved ArrowArrayStream, and the release of the C
// allocation it was moved from.
//
// The C allocation keeps the source Arrow table alive and the stream reads
// from that table by reference, so free runs only after the reader is
// released. Records read from the stream hold their own buffers and stay
// valid after both.
type arrowStream struct {
	reader array.RecordReader
	free   func()
}

// fetchArrowStream runs one qdb_bulk_reader_get_data_arrow call and moves
// the result into an arrow-go record reader. Returns ErrIteratorEnd at end
// of data. The caller drains the stream with drain, which releases it.
func (r *Reader) fetchArrowStream() (arrowStream, error) {
	var stream *C.struct_ArrowArrayStream

	errCode := C.qdb_bulk_reader_get_data_arrow(r.state, &stream, C.qdb_size_t(r.options.batchSize))
	err := wrapError(errCode, "reader_fetch_arrow", "batch_size", r.options.batchSize)
	if err != nil {
		if stream != nil {
			qdbReleasePointer(r.handle, unsafe.Pointer(stream))
		}

		return arrowStream{}, err
	}

	if stream == nil {
		return arrowStream{}, wrapError(C.qdb_e_invalid_reply, "reader_fetch_arrow", errorDetailKey, "no stream returned")
	}
	free := releaseCPtr(r.handle, unsafe.Pointer(stream))

	// cdata's struct type shares the C layout of struct ArrowArrayStream;
	// the cast is the handoff. The schema is read from the stream itself.
	rr, err := cdata.ImportCRecordReader((*cdata.CArrowArrayStream)(unsafe.Pointer(stream)), nil)
	if err != nil {
		free()

		return arrowStream{}, wrapError(C.qdb_e_incompatible_type, "reader_arrow_import", errorDetailKey, err.Error())
	}

	reader, ok := rr.(array.RecordReader)
	if !ok {
		free()

		return arrowStream{}, wrapError(C.qdb_e_incompatible_type, "reader_arrow_import", errorDetailKey, "stream reader lacks Release")
	}

	return arrowStream{reader: reader, free: free}, nil
}

// drain yields every record in the stream, in order. Each record is
// retained before it is handed out, so the receiver owns one reference.
// Returns false when yield asked to stop or an error step was yielded. The
// stream is released before returning, in every case.
func (s arrowStream) drain(yield func(arrow.RecordBatch, error) bool) bool {
	defer s.free()
	defer s.reader.Release()

	for s.reader.Next() {
		rec := s.reader.RecordBatch()
		rec.Retain()
		if !yield(rec, nil) {
			return false
		}
	}

	err := s.reader.Err()
	if err != nil {
		yield(nil, wrapError(C.qdb_e_incompatible_type, "reader_arrow_import", errorDetailKey, err.Error()))

		return false
	}

	return true
}

// Arrow returns the remaining rows as Arrow record batches.
//
// Returns:
//
//	iter.Seq2[arrow.RecordBatch, error]: one batch per step; the last step
//	carries a non-nil error when the read or the Arrow import failed
//
// The caller owns every yielded batch and must Release it exactly once. A
// batch holds no reference to the reader or the handle and stays valid
// after the next step, after Close and after the handle is closed. On an
// error step the batch is nil.
//
// Schema: without WithColumns the fields are "$table" (utf8), "$timestamp"
// (timestamp[ns], no zone) and then every data column; with WithColumns
// the requested order, specials only when named. Data columns are
// nullable. Timestamps carry no zone, as in Query.FetchArrow.
//
// One sequence per Reader. A second Chunks, Arrow or Next call after the
// first step yields ErrInvalidIterator. Breaking out of the loop is safe;
// the reader is still closed by Close.
//
// Example:
//
//	for rec, err := range rd.Arrow() {
//	    if err != nil {
//	        return err
//	    }
//	    process(rec)
//	    rec.Release()
//	}
func (r *Reader) Arrow() iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		err := r.claimCursor(cursorBySequence)
		if err != nil {
			yield(nil, err)

			return
		}
		defer func() { r.done = true }()

		for {
			stream, err := r.fetchArrowStream()
			if errors.Is(err, ErrIteratorEnd) {
				return
			}
			if err != nil {
				yield(nil, err)

				return
			}
			if !stream.drain(yield) {
				return
			}
		}
	}
}
