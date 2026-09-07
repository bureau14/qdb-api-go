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


	qdb_timespec_t get_timestamp_from_payload(const qdb_point_result_t * result)
	{

		return (qdb_timespec_t)result->payload.timestamp.value;
	}
*/
import "C"

import (
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
	QueryResultTimestamp QueryResultValueType = C.qdb_query_result_timestamp
	QueryResultCount     QueryResultValueType = C.qdb_query_result_count
)

// QueryPoint : a variadic structure holding the result type as well as the
// result value
type QueryPoint C.qdb_point_result_t

// QueryRow : query result table row
type QueryRow []QueryPoint

// QueryRows : query result table rows
type QueryRows []*QueryPoint

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
	case C.qdb_query_result_timestamp:
		output.value = TimespecToStructG(C.get_timestamp_from_payload(result))
	case C.qdb_query_result_count:
		output.value = int64(C.get_count_from_payload(result))
	case C.qdb_query_result_none:
		output.value = nil
	}

	return output
}

// GetDouble : retrieve a double from the interface
func (r *QueryPoint) GetDouble() (float64, error) {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	if result._type == C.qdb_query_result_double {
		return float64(C.get_double_from_payload(result)), nil
	}

	return 0, wrapError(C.qdb_e_incompatible_type, "query_point_get_double", "wrong_type", "expected_double")
}

// GetBlob : retrieve a blob from the interface
func (r *QueryPoint) GetBlob() ([]byte, error) {
	if r._type == C.qdb_query_result_blob {
		result := (*C.qdb_point_result_t)(unsafe.Pointer(r))

		return getBlobUnsafe(result), nil
	}

	return []byte{}, wrapError(C.qdb_e_incompatible_type, "query_point_get_blob", "wrong_type", "expected_blob")
}

// GetInt64 : retrieve an int64 from the interface
func (r *QueryPoint) GetInt64() (int64, error) {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	if result._type == C.qdb_query_result_int64 {
		return int64(C.get_int64_from_payload(result)), nil
	}

	return 0, wrapError(C.qdb_e_incompatible_type, "query_point_get_int64", "wrong_type", "expected_int64")
}

// GetString : retrieve a string from the interface
func (r *QueryPoint) GetString() (string, error) {
	if r._type == C.qdb_query_result_string {
		result := (*C.qdb_point_result_t)(unsafe.Pointer(r))

		return getStringUnsafe(result), nil
	}

	return "", wrapError(C.qdb_e_incompatible_type, "query_point_get_string", "wrong_type", "expected_string")
}

// GetTimestamp : retrieve a timestamp from the interface
func (r *QueryPoint) GetTimestamp() (time.Time, error) {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	if result._type == C.qdb_query_result_timestamp {
		return TimespecToStructG(C.get_timestamp_from_payload(result)), nil
	}

	return time.Unix(-1, -1), wrapError(C.qdb_e_incompatible_type, "query_point_get_timestamp", "wrong_type", "expected_timestamp")
}

// GetCount : retrieve the count from the interface
func (r *QueryPoint) GetCount() (int64, error) {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	if result._type == C.qdb_query_result_count {
		return int64(C.get_count_from_payload(result)), nil
	}

	return 0, wrapError(C.qdb_e_incompatible_type, "query_point_get_count", "wrong_type", "expected_count")
}

// Query : query object
type Query struct {
	HandleType
	query string
}

// Execute runs the query against the cluster.
//
// Args:
//
//	None
//
// Returns:
//
//	*QueryResult: Result set, or nil when the statement produces none (e.g. DDL)
//	error: Query error, if any
//
// A non-nil result is owned by the caller and must be released with Close,
// including when an error is returned alongside it. Close is nil-safe, so it
// can be deferred before checking the error.
//
// The result is a view over C memory: every cell is decoded on access and
// nothing obtained from it may outlive Close. Callers who want Go-owned,
// column-oriented data with no Close obligation use Fetch instead, which
// copies the result into a QueryResultSet and releases it before returning.
//
// Example:
//
//	result, err := h.Query("SELECT * FROM measurements").Execute()
//	defer result.Close()
//	if err != nil {
//	    return err
//	}
func (q Query) Execute() (*QueryResult, error) {
	query := convertToCharStar(q.query)
	defer releaseCharStar(query)
	r := QueryResult{handle: q.HandleType}
	err := C.qdb_query(q.handle, query, &r.result)
	if r.result == nil {
		return nil, wrapError(err, "query_execute", "query", q.query)
	}

	return &r, wrapError(err, "query_execute", "query", q.query)
}
