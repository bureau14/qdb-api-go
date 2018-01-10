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

// TsRange : timeseries range with begin and end timestamp
type TsRange struct {
	begin  time.Time
	end    time.Time
	filter TsFilter
}

// Begin : returns the start of the time range
func (t TsRange) Begin() time.Time {
	return t.begin
}

// End : returns the end of the time range
func (t TsRange) End() time.Time {
	return t.end
}

// NewRange : creates a time range
func NewRange(begin, end time.Time) TsRange {
	return TsRange{begin: begin, end: end}
}

// NewFilteredRange : creates a time range with additional filters
func NewFilteredRange(begin, end time.Time, filter TsFilter) TsRange {
	return TsRange{begin: begin, end: end, filter: filter}
}

// :: internals
func (t TsRange) toStructC() C.qdb_ts_filtered_range_t {
	r := C.qdb_ts_range_t{begin: toQdbTimespec(t.begin), end: toQdbTimespec(t.end)}
	f := C.qdb_ts_filter_t{_type: C.qdb_ts_filter_type_t(t.filter.filterType)}
	switch t.filter.filterType {
	case FilterNone:
	case FilterSample:
		*(*C.qdb_size_t)(unsafe.Pointer(&f.params)) = C.qdb_size_t(t.filter.sampleSize)
	case RangeFilterType(FilterDoubleInsideRange):
		fallthrough
	case RangeFilterType(FilterDoubleOutsideRange):
		*(*C.double_range)(unsafe.Pointer(&f.params)) = C.double_range{C.double(t.filter.span.min), C.double(t.filter.span.max)}
	default:
	}
	rf := C.qdb_ts_filtered_range_t{_range: r, filter: f}
	return rf
}

func (t C.qdb_ts_filtered_range_t) toStructG() TsRange {
	r := NewRange(t._range.begin.toStructG(), t._range.end.toStructG())
	r.filter.filterType = RangeFilterType(t.filter._type)
	switch r.filter.filterType {
	case FilterNone:
	case FilterSample:
		r.filter.sampleSize = *(*int64)(unsafe.Pointer(&t.filter.params))
	case RangeFilterType(FilterDoubleInsideRange):
		fallthrough
	case RangeFilterType(FilterDoubleOutsideRange):
		doubles := *(*C.double_range)(unsafe.Pointer(&t.filter.params))
		r.filter.span.min = float64(doubles.min)
		r.filter.span.max = float64(doubles.max)
	default:
	}
	return r
}

func rangeArrayToC(rs ...TsRange) *C.qdb_ts_filtered_range_t {
	if len(rs) == 0 {
		return nil
	}
	var ranges []C.qdb_ts_filtered_range_t
	for _, r := range rs {
		ranges = append(ranges, r.toStructC())
	}
	return &ranges[0]
}

// :: Start Range Filter ::

// RangeFilterType : A filter type for qdb timeseries range
type RangeFilterType int64

// RangeFilterTypeDouble : A filter type for qdb timeseries range specific to double columns
type RangeFilterTypeDouble RangeFilterType

// RangeFilterType Values
//	FilterNone : No filter type
//	FilterUnique : Not implemented
//	FilterSample : Not implemented
//	FilterDoubleInsideRange : Not implemented
//	FilterDoubleOutsideRange : Not implemented
const (
	FilterNone               RangeFilterType       = C.qdb_ts_filter_none
	FilterUnique             RangeFilterType       = C.qdb_ts_filter_unique
	FilterSample             RangeFilterType       = C.qdb_ts_filter_sample
	FilterDoubleInsideRange  RangeFilterTypeDouble = C.qdb_ts_filter_double_inside_range
	FilterDoubleOutsideRange RangeFilterTypeDouble = C.qdb_ts_filter_double_outside_range
)

type doubleSpan struct {
	min, max float64
}

// TsFilter : A way to filter results on a range
type TsFilter struct {
	filterType RangeFilterType
	sampleSize int64
	span       doubleSpan
}

// NewFilter : creates a new filter
func NewFilter() TsFilter {
	return TsFilter{}
}

// Unique : Not implemented
//	Override information set by other filter functions if called before
//	This API will soon be updated
func (t TsFilter) Unique() TsFilter {
	t.filterType = FilterUnique
	return t
}

// Sample : Not implemented
//	Override information set by other filter functions if called before
//	This API will soon be updated
func (t TsFilter) Sample(sampleSize int64) TsFilter {
	t.sampleSize = sampleSize
	t.filterType = FilterSample
	return t
}

// DoubleLimits : Not implemented
//	Override information set by other filter functions if called before
//	This API will soon be updated
func (t TsFilter) DoubleLimits(min, max float64, filterType RangeFilterTypeDouble) TsFilter {
	t.span = doubleSpan{min: min, max: max}
	t.filterType = RangeFilterType(filterType)
	return t
}
