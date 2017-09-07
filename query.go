package qdb

/*
	#include <qdb/query.h>
	#include <stdlib.h>
*/
import "C"
import (
	"bytes"
	"errors"
	"strconv"
	"unsafe"
)

// Query : a building type to execute a query
// Retrieves all entries’ aliases that match the specified query.
// For the complete grammar, please refer to the documentation.
// Queries are transactional.
// The complexity of this function is dependent on the complexity of the query.
type Query struct {
	HandleType
	tagsExcluded []string
	tags         []string
	types        []string
}

// Tag : Adds a tag to include into the current query results
func (q *Query) Tag(t string) *Query {
	q.tags = append(q.tags, t)
	return q
}

// NotTag : Adds a tag to exclude from the current query results
func (q *Query) NotTag(t string) *Query {
	q.tagsExcluded = append(q.tagsExcluded, t)
	return q
}

// Type : Restrict the query results to a particular type
func (q *Query) Type(t string) *Query {
	q.types = append(q.types, t)
	return q
}

func (q Query) buildQuery() (string, error) {
	var query bytes.Buffer
	for idx, t := range q.tags {
		if idx != 0 {
			query.WriteString(" and ")
		}
		query.WriteString("tag=")
		query.WriteString(strconv.Quote(t))
	}
	if query.Len() == 0 {
		return string(""), errors.New("Query should have at least one valid tag")
	}
	for _, t := range q.tagsExcluded {
		query.WriteString(" and not tag=")
		query.WriteString(strconv.Quote(t))
	}
	for _, t := range q.types {
		query.WriteString(" and type=")
		query.WriteString(t)
	}
	return query.String(), nil
}

// Execute : Execute the current query
func (q Query) Execute() ([]string, error) {
	query, err := q.buildQuery()
	if err != nil {
		return nil, err
	}
	return q.ExecuteString(query)
}

// ExecuteString : Execute a string query immediately
func (q Query) ExecuteString(query string) ([]string, error) {
	var aliasCount C.size_t
	var aliases **C.char
	err := C.qdb_query(q.handle, C.CString(query), &aliases, &aliasCount)
	if err == 0 {
		length := int(aliasCount)
		output := make([]string, length)
		if aliasCount > 0 {
			defer q.Release(unsafe.Pointer(aliases))
			tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(aliases))[:length:length]
			for i, s := range tmpslice {
				output[i] = C.GoString(s)
			}
		}
		return output, nil
	}
	return nil, ErrorType(err)
}
