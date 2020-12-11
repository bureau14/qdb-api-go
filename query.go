package qdb

/*
	#include <qdb/query.h>

	qdb_size_t get_count_from_payload(const qdb_point_result_t * result)
	{
		return (qdb_size_t)result->payload.count.value;
	}

	qdb_int_t get_int64_from_payload(const qdb_point_result_t * result)
	{
		return (qdb_int_t)result->payload.int64_.value;
	}

	double get_double_from_payload(const qdb_point_result_t * result)
	{
		return (double)result->payload.double_.value;
	}

	void get_blob_from_payload(const qdb_point_result_t *result, const void ** content, qdb_size_t *length)
	{
		*content = result->payload.blob.content;
		*length = result->payload.blob.content_length;
	}

	void get_string_from_payload(const qdb_point_result_t *result, const char ** content, qdb_size_t *length)
	{
		*content = result->payload.string.content;
		*length = result->payload.string.content_length;
	}

	void get_symbol_from_payload(const qdb_point_result_t *result, const char ** content, qdb_size_t *length)
	{
		*content = result->payload.symbol.content;
		*length = result->payload.symbol.content_length;
	}


	qdb_timespec_t get_timestamp_from_payload(const qdb_point_result_t * result)
	{
		return (qdb_timespec_t)result->payload.timestamp.value;
	}
*/
import "C"
import (
	"math"
	"time"
	"unsafe"
)

// QueryResultValueType : an enum of possible query point result types
type QueryResultValueType int64

// QueryResultNone : query result value none
// QueryResultDouble : query result value double
// QueryResultBlob : query result value blob
// QueryResultInt64 : query result value int64
// QueryResultString : query result value string
// QueryResultSymbol : query result value symbol
// QueryResultTimestamp : query result value timestamp
// QueryResultCount : query result value count
const (
	QueryResultNone      QueryResultValueType = C.qdb_query_result_none
	QueryResultDouble    QueryResultValueType = C.qdb_query_result_double
	QueryResultBlob      QueryResultValueType = C.qdb_query_result_blob
	QueryResultInt64     QueryResultValueType = C.qdb_query_result_int64
	QueryResultString    QueryResultValueType = C.qdb_query_result_string
	QueryResultSymbol    QueryResultValueType = C.qdb_query_result_symbol
	QueryResultTimestamp QueryResultValueType = C.qdb_query_result_timestamp
	QueryResultCount     QueryResultValueType = C.qdb_query_result_count
)

// QueryPointResult : a query result point
type QueryPointResult struct {
	valueType QueryResultValueType
	value     interface{}
}

// Type : gives the type of the query point result
func (r QueryPointResult) Type() QueryResultValueType {
	return r.valueType
}

// Value : gives the interface{} value of the query point result
func (r QueryPointResult) Value() interface{} {
	return r.value
}

func getBlobUnsafe(result *C.qdb_point_result_t) []byte {
	var content unsafe.Pointer
	var contentLength C.qdb_size_t
	C.get_blob_from_payload(result, &content, &contentLength)
	return C.GoBytes(content, C.int(contentLength))
}

func getStringUnsafe(result *C.qdb_point_result_t) string {
	var content *C.char
	var contentLength C.qdb_size_t
	C.get_string_from_payload(result, &content, &contentLength)
	return C.GoStringN(content, C.int(contentLength))
}

func getSymbolUnsafe(result *C.qdb_point_result_t) string {
	var content *C.char
	var contentLength C.qdb_size_t
	C.get_symbol_from_payload(result, &content, &contentLength)
	return C.GoStringN(content, C.int(contentLength))
}

// Get : retrieve the raw interface
func (r *QueryPoint) Get() QueryPointResult {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	output := QueryPointResult{valueType: QueryResultValueType(result._type)}

	switch output.valueType {
	case C.qdb_query_result_double:
		output.value = float64(C.get_double_from_payload(result))
	case C.qdb_query_result_blob:
		output.value = getBlobUnsafe(result)
	case C.qdb_query_result_int64:
		output.value = int64(C.get_int64_from_payload(result))
	case C.qdb_query_result_string:
		output.value = getStringUnsafe(result)
	case C.qdb_query_result_symbol:
		output.value = getSymbolUnsafe(result)
	case C.qdb_query_result_timestamp:
		output.value = C.get_timestamp_from_payload(result).toStructG()
	case C.qdb_query_result_count:
		output.value = int64(C.get_count_from_payload(result))
	}
	return output
}

// GetDouble : retrieve a double from the interface
func (r *QueryPoint) GetDouble() (float64, error) {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	if result._type == C.qdb_query_result_double {
		return float64(C.get_double_from_payload(result)), nil
	}
	return 0, makeErrorOrNil(C.qdb_e_operation_not_permitted)
}

// GetBlob : retrieve a double from the interface
func (r *QueryPoint) GetBlob() ([]byte, error) {
	if r._type == C.qdb_query_result_blob {
		result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
		return getBlobUnsafe(result), nil
	}
	return []byte{}, makeErrorOrNil(C.qdb_e_operation_not_permitted)
}

// GetInt64 : retrieve an int64 from the interface
func (r *QueryPoint) GetInt64() (int64, error) {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	if result._type == C.qdb_query_result_int64 {
		return int64(C.get_int64_from_payload(result)), nil
	}
	return 0, makeErrorOrNil(C.qdb_e_operation_not_permitted)
}

// GetString : retrieve a string from the interface
func (r *QueryPoint) GetString() (string, error) {
	if r._type == C.qdb_query_result_string {
		result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
		return getStringUnsafe(result), nil
	}
	return "", makeErrorOrNil(C.qdb_e_operation_not_permitted)
}

// GetSymbol : retrieve a symbol from the interface
func (r *QueryPoint) GetSymbol() (string, error) {
	if r._type == C.qdb_query_result_symbol {
		result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
		return getSymbolUnsafe(result), nil
	}
	return "", makeErrorOrNil(C.qdb_e_operation_not_permitted)
}

// GetTimestamp : retrieve a timestamp from the interface
func (r *QueryPoint) GetTimestamp() (time.Time, error) {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	if result._type == C.qdb_query_result_timestamp {
		return C.get_timestamp_from_payload(result).toStructG(), nil
	}
	return time.Unix(-1, -1), makeErrorOrNil(C.qdb_e_operation_not_permitted)
}

// GetCount : retrieve the count from the interface
func (r *QueryPoint) GetCount() (int64, error) {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	if result._type == C.qdb_query_result_count {
		return int64(C.get_count_from_payload(result)), nil
	}
	return 0, makeErrorOrNil(C.qdb_e_operation_not_permitted)
}

// QueryPoint : a variadic structure holding the result type as well as the
// result value
type QueryPoint C.qdb_point_result_t

// QueryRow : query result table row
type QueryRow []QueryPoint

// QueryRows : query result table rows
type QueryRows []*QueryPoint

// QueryResult : a query result
type QueryResult struct {
	result *C.qdb_query_result_t
}

// ScannedPoints : number of points scanned
//	The actual number of scanned points may be greater
func (r QueryResult) ScannedPoints() int64 {
	return int64(r.result.scanned_point_count)
}

func queryPointArrayToSlice(row *QueryPoint, length int64) []QueryPoint {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(QueryPoint{})]QueryPoint)(unsafe.Pointer(row))[:length:length]
}

func qdbPointResultStarArrayToSlice(rows **C.qdb_point_result_t, length int64) []*QueryPoint {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof((*C.qdb_point_result_t)(nil))]*QueryPoint)(unsafe.Pointer(rows))[:length:length]
}

func qdbStringArrayToSlice(strings *C.qdb_string_t, length int64) []C.qdb_string_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_string_t{})]C.qdb_string_t)(unsafe.Pointer(strings))[:length:length]
}

// Columns : create columns from a row
func (r QueryResult) Columns(row *QueryPoint) QueryRow {
	count := int64(r.result.column_count)
	return queryPointArrayToSlice(row, count)
}

// Rows : get rows of a query table result
func (r QueryResult) Rows() QueryRows {
	count := int64(r.result.row_count)
	if count == 0 {
		return []*QueryPoint{}
	}
	return qdbPointResultStarArrayToSlice(r.result.rows, count)
}

// ColumnsNames : get the number of columns names of each row
func (r QueryResult) ColumnsNames() []string {
	count := int64(r.result.column_count)
	result := make([]string, count)
	rawNames := qdbStringArrayToSlice(r.result.column_names, count)
	for i := range rawNames {
		result[i] = C.GoString(rawNames[i].data)
	}
	return result
}

// ColumnsCount : get the number of columns of each row
func (r QueryResult) ColumnsCount() int64 {
	return int64(r.result.column_count)
}

// RowCount : the number of returned rows
func (r QueryResult) RowCount() int64 {
	if r.result == nil {
		return 0
	}
	return int64(r.result.row_count)
}

// Query : query object
type Query struct {
	HandleType
	query string
}

// Execute : execute a query
func (q Query) Execute() (*QueryResult, error) {
	query := convertToCharStar(q.query)
	defer releaseCharStar(query)
	var r QueryResult
	err := C.qdb_query(q.handle, query, &r.result)
	return &r, makeErrorOrNil(err)
}
