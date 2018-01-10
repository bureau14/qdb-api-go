package qdb

/*
	#include <qdb/ts.h>

	typedef struct
	{
		double min;
		double max;
	} double_range;
*/
import "C"
import (
	"time"
	"unsafe"
)

// TsDoublePoint : timestamped double data point
type TsDoublePoint struct {
	timestamp time.Time
	content   float64
}

// Timestamp : return data point timestamp
func (t TsDoublePoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsDoublePoint) Content() float64 {
	return t.content
}

// NewTsDoublePoint : Create new timeseries double point
func NewTsDoublePoint(timestamp time.Time, value float64) TsDoublePoint {
	return TsDoublePoint{timestamp, value}
}

// :: internals
func (t TsDoublePoint) toStructC() C.qdb_ts_double_point {
	return C.qdb_ts_double_point{toQdbTimespec(t.timestamp), C.double(t.content)}
}

func (t C.qdb_ts_double_point) toStructG() TsDoublePoint {
	return TsDoublePoint{t.timestamp.toStructG(), float64(t.value)}
}

func doublePointArrayToC(pts ...TsDoublePoint) *C.qdb_ts_double_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_double_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}
	return &points[0]
}

func doublePointArrayToGo(points *C.qdb_ts_double_point, pointsCount C.qdb_size_t) []TsDoublePoint {
	length := int(pointsCount)
	output := make([]TsDoublePoint, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_double_point)(unsafe.Pointer(points))[:length:length]
		for i, s := range tmpslice {
			output[i] = s.toStructG()
		}
	}
	return output
}

// :: :: End - Double Point ::

// :: :: Start - Blob Point ::

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

// NewTsBlobPoint : Create new timeseries double point
func NewTsBlobPoint(timestamp time.Time, value []byte) TsBlobPoint {
	return TsBlobPoint{timestamp, value}
}

// :: internals

// TODO(vianney) : do a better conversion without losing the capacity to pass a pointer
// solution may be in go 1.7: func C.CBytes([]byte) unsafe.Pointer
func (t TsBlobPoint) toStructC() C.qdb_ts_blob_point {
	dataSize := C.qdb_size_t(len(t.content))
	data := unsafe.Pointer(C.CString(string(t.content)))
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

func blobPointArrayToGo(points *C.qdb_ts_blob_point, pointsCount C.qdb_size_t) []TsBlobPoint {
	length := int(pointsCount)
	output := make([]TsBlobPoint, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_blob_point)(unsafe.Pointer(points))[:length:length]
		for i, s := range tmpslice {
			output[i] = s.toStructG()
		}
	}
	return output
}
