package qdb

/*
	#include <qdb/ts.h>
	#include <stdlib.h>
*/
import "C"
import (
	"math"
	"time"
	"unsafe"
)

// TsBlobPoint : timestamped data
type TsBlobPoint struct {
	timestamp time.Time
	content   []byte
}

// Timestamp : return data point timestamp
func (t TsBlobPoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsBlobPoint) Content() []byte {
	return t.content
}

// NewTsBlobPoint : Create new timeseries blob point
func NewTsBlobPoint(timestamp time.Time, value []byte) TsBlobPoint {
	return TsBlobPoint{timestamp, value}
}

// :: internals

// TODO(vianney) : do a better conversion without losing the capacity to pass a pointer
// solution may be in go 1.7: func C.CBytes([]byte) unsafe.Pointer
func (t TsBlobPoint) toStructC() C.qdb_ts_blob_point {
	dataSize := C.qdb_size_t(len(t.content))
	data := unsafe.Pointer(convertToCharStar(string(t.content)))
	return C.qdb_ts_blob_point{toQdbTimespec(t.timestamp), data, dataSize}
}

func (t C.qdb_ts_blob_point) toStructG() TsBlobPoint {
	return TsBlobPoint{t.timestamp.toStructG(), C.GoBytes(t.content, C.int(t.content_length))}
}

func blobPointArrayToC(pts ...TsBlobPoint) *C.qdb_ts_blob_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_blob_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}
	return &points[0]
}

func releaseBlobPointArray(points *C.qdb_ts_blob_point, length int) {
	if length > 0 {
		slice := blobPointArrayToSlice(points, length)
		for _, s := range slice {
			C.free(unsafe.Pointer(s.content))
		}
	}
}

func blobPointArrayToSlice(points *C.qdb_ts_blob_point, length int) []C.qdb_ts_blob_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_blob_point{})]C.qdb_ts_blob_point)(unsafe.Pointer(points))[:length:length]
}

func blobPointArrayToGo(points *C.qdb_ts_blob_point, pointsCount C.qdb_size_t) []TsBlobPoint {
	length := int(pointsCount)
	output := make([]TsBlobPoint, length)
	if length > 0 {
		slice := blobPointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = s.toStructG()
		}
	}
	return output
}

// TsBlobColumn : a time series blob column
type TsBlobColumn struct {
	tsColumn
}

// BlobColumn : create a column object
func (entry TimeseriesEntry) BlobColumn(columnName string) TsBlobColumn {
	return TsBlobColumn{tsColumn{NewTsColumnInfo(columnName, TsColumnBlob), entry}}
}

// Insert blob points into a timeseries
func (column TsBlobColumn) Insert(points ...TsBlobPoint) error {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	contentCount := C.qdb_size_t(len(points))
	content := blobPointArrayToC(points...)
	defer releaseBlobPointArray(content, len(points))
	err := C.qdb_ts_blob_insert(column.parent.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
}

// EraseRanges : erase all points in the specified ranges
func (column TsBlobColumn) EraseRanges(rgs ...TsRange) (uint64, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	erasedCount := C.qdb_uint_t(0)
	err := C.qdb_ts_erase_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &erasedCount)
	return uint64(erasedCount), makeErrorOrNil(err)
}

// GetRanges : Retrieves blobs in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (column TsBlobColumn) GetRanges(rgs ...TsRange) ([]TsBlobPoint, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_blob_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_blob_get_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer column.parent.Release(unsafe.Pointer(points))
		return blobPointArrayToGo(points, pointsCount), nil
	}
	return nil, ErrorType(err)
}

// TsBlobAggregation : Aggregation of double type
type TsBlobAggregation struct {
	kind  TsAggregationType
	rng   TsRange
	count int64
	point TsBlobPoint
}

// Type : returns the type of the aggregation
func (t TsBlobAggregation) Type() TsAggregationType {
	return t.kind
}

// Range : returns the range of the aggregation
func (t TsBlobAggregation) Range() TsRange {
	return t.rng
}

// Count : returns the number of points aggregated into the result
func (t TsBlobAggregation) Count() int64 {
	return t.count
}

// Result : result of the aggregation
func (t TsBlobAggregation) Result() TsBlobPoint {
	return t.point
}

// NewBlobAggregation : Create new timeseries blob aggregation
func NewBlobAggregation(kind TsAggregationType, rng TsRange) *TsBlobAggregation {
	return &TsBlobAggregation{kind, rng, 0, TsBlobPoint{}}
}

// :: internals
func (t TsBlobAggregation) toStructC() C.qdb_ts_blob_aggregation_t {
	var cAgg C.qdb_ts_blob_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.kind)
	cAgg._range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_blob_aggregation_t) toStructG() TsBlobAggregation {
	var gAgg TsBlobAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t._range.toStructG()
	gAgg.count = int64(t.count)
	gAgg.point = t.result.toStructG()
	return gAgg
}

func blobAggregationArrayToC(ags ...*TsBlobAggregation) *C.qdb_ts_blob_aggregation_t {
	if len(ags) == 0 {
		return nil
	}
	var blobAggregations []C.qdb_ts_blob_aggregation_t
	for _, ag := range ags {
		blobAggregations = append(blobAggregations, ag.toStructC())
	}
	return &blobAggregations[0]
}

func blobAggregationArrayToSlice(aggregations *C.qdb_ts_blob_aggregation_t, length int) []C.qdb_ts_blob_aggregation_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_blob_aggregation_t{})]C.qdb_ts_blob_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
}

func blobAggregationArrayToGo(aggregations *C.qdb_ts_blob_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsBlobAggregation) []TsBlobAggregation {
	length := int(aggregationsCount)
	output := make([]TsBlobAggregation, length)
	if length > 0 {
		slice := blobAggregationArrayToSlice(aggregations, length)
		for i, s := range slice {
			*aggs[i] = s.toStructG()
			output[i] = s.toStructG()
		}
	}
	return output
}

// Aggregate : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (column TsBlobColumn) Aggregate(aggs ...*TsBlobAggregation) ([]TsBlobAggregation, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	aggregations := blobAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsBlobAggregation
	err := C.qdb_ts_blob_aggregate(column.parent.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = blobAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}
	return output, makeErrorOrNil(err)
}

// GetBlob : gets a blob in row
func (t *TsBulk) GetBlob() ([]byte, error) {
	var content unsafe.Pointer
	defer t.h.Release(content)
	var contentLength C.qdb_size_t
	err := C.qdb_ts_row_get_blob(t.table, C.qdb_size_t(t.index), &content, &contentLength)

	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	t.index++
	return output, makeErrorOrNil(err)
}

// RowSetBlob : Set blob at specified index in current row
func (t *TsBatch) RowSetBlob(index int64, content []byte) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}
	return makeErrorOrNil(C.qdb_ts_batch_row_set_blob(t.table, valueIndex, contentPtr, contentSize))
}

// RowSetBlobNoCopy : Set blob at specified index in current row without copying it
func (t *TsBatch) RowSetBlobNoCopy(index int64, content []byte) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}
	return makeErrorOrNil(C.qdb_ts_batch_row_set_blob_no_copy(t.table, valueIndex, contentPtr, contentSize))
}
