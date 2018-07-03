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
// QueryResultTimestamp : query result value timestamp
// QueryResultCount : query result value count
const (
	QueryResultNone      QueryResultValueType = C.qdb_query_result_none
	QueryResultDouble    QueryResultValueType = C.qdb_query_result_double
	QueryResultBlob      QueryResultValueType = C.qdb_query_result_blob
	QueryResultInt64     QueryResultValueType = C.qdb_query_result_int64
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

func getBlobUnsafe(r *QueryPoint) []byte {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	var content unsafe.Pointer
	var contentLength C.qdb_size_t
	C.get_blob_from_payload(result, &content, &contentLength)
	return C.GoBytes(content, C.int(contentLength))
}

// Get : retrieve the raw interface
func (r *QueryPoint) Get() QueryPointResult {
	result := (*C.qdb_point_result_t)(unsafe.Pointer(r))
	output := QueryPointResult{valueType: QueryResultValueType(result._type)}

	switch output.valueType {
	case C.qdb_query_result_double:
		output.value = float64(C.get_double_from_payload(result))
	case C.qdb_query_result_blob:
		output.value = getBlobUnsafe(r)
	case C.qdb_query_result_int64:
		output.value = int64(C.get_int64_from_payload(result))
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
		return getBlobUnsafe(r), nil
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

type QueryPoint C.qdb_point_result_t

// QueryRow : query result table row
type QueryRow []QueryPoint

// QueryRows : query result table rows
type QueryRows []*QueryPoint

// QueryTable : query result table
type QueryTable C.qdb_table_result_t

// QueryTables : query result tables
type QueryTables []QueryTable

// Columns : create columns from a row
func (r QueryTable) Columns(row *QueryPoint) QueryRow {
	count := int64(r.columns_count)
	return (*[1 << 30]QueryPoint)(unsafe.Pointer(row))[:count:count]
}

// Rows : get rows of a query table result
func (r QueryTable) Rows() QueryRows {
	count := int64(r.rows_count)
	return (*[1 << 30]*QueryPoint)(unsafe.Pointer(r.rows))[:count:count]
}

// Name : get table name
func (r QueryTable) Name() string {
	return C.GoString(r.table_name.data)
}

// ColumnsNames : get the number of columns names of each row
func (r QueryTable) ColumnsNames() []string {
	count := int64(r.columns_count)
	result := make([]string, count)
	rawNames := (*[1 << 30]C.qdb_string_t)(unsafe.Pointer(r.columns_names))[:count:count]
	for i := range rawNames {
		result[i] = C.GoString(rawNames[i].data)
	}
	return result
}

// ColumnsCount : get the number of columns of each row
func (r QueryTable) ColumnsCount() int64 {
	return int64(r.columns_count)
}

// QueryResult : a query result
type QueryResult struct {
	result *C.qdb_query_result_t
}

// Tables : get tables of a query result
func (r QueryResult) Tables() QueryTables {
	count := int64(r.result.tables_count)
	tables := (*[1 << 30]QueryTable)(unsafe.Pointer(r.result.tables))[:count:count]
	return tables
}

// TablesCount : get the number of tables of a query result
func (r QueryResult) TablesCount() int64 {
	return int64(r.result.tables_count)
}

// ScannedRows : number of rows scanned
//	The actual number of scanned rows may be greater
func (r QueryResult) ScannedRows() int64 {
	return int64(r.result.scanned_rows_count)
}

// QueryExp : Experimental query
type QueryExp struct {
	HandleType
	query string
}

// Execute : execute a query
func (q QueryExp) Execute() (*QueryResult, error) {
	query := convertToCharStar(q.query)
	defer releaseCharStar(query)
	var r QueryResult
	err := C.qdb_exp_query(q.handle, query, &r.result)
	return &r, makeErrorOrNil(err)
}
