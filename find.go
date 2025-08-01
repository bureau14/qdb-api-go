package qdb

/*
	#include <qdb/query.h>
	#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"errors"
	"unsafe"
)

// Find : a building type to execute a query
// Retrieves all entries’ aliases that match the specified query.
// For the complete grammar, please refer to the documentation.
// Queries are transactional.
// The complexity of this function is dependent on the complexity of the query.
type Find struct {
	HandleType
	tagsExcluded []string
	tags         []string
	types        []string
}

// Tag : Adds a tag to include into the current query results
func (q *Find) Tag(t string) *Find {
	q.tags = append(q.tags, t)
	return q
}

// NotTag : Adds a tag to exclude from the current query results
func (q *Find) NotTag(t string) *Find {
	q.tagsExcluded = append(q.tagsExcluded, t)
	return q
}

// Type : Restrict the query results to a particular type
func (q *Find) Type(t string) *Find {
	q.types = append(q.types, t)
	return q
}

func (q Find) buildQuery() (string, error) {
	var query bytes.Buffer
	query.WriteString("find(")
	for idx, t := range q.tags {
		if idx != 0 {
			query.WriteString(" and ")
		}
		query.WriteString("tag='")
		query.WriteString(t)
		query.WriteString("'")
	}
	if query.Len() == len(string("find(")) {
		return string(""), errors.New("query should have at least one valid tag")
	}
	for _, t := range q.tagsExcluded {
		query.WriteString(" and not tag='")
		query.WriteString(t)
		query.WriteString("'")
	}
	for _, t := range q.types {
		query.WriteString(" and type=")
		query.WriteString(t)
	}
	query.WriteString(")")
	return query.String(), nil
}

// Execute : Execute the current query
func (q Find) Execute() ([]string, error) {
	query, err := q.buildQuery()
	if err != nil {
		return nil, err
	}
	return q.ExecuteString(query)
}

// ExecuteString : Execute a string query immediately
func (q Find) ExecuteString(query string) ([]string, error) {
	cQuery := convertToCharStar(query)
	defer releaseCharStar(cQuery)
	var aliasCount C.size_t
	var aliases **C.char
	err := C.qdb_query_find(q.handle, cQuery, &aliases, &aliasCount)
	if err == 0 {
		length := int(aliasCount)
		output := make([]string, length)
		if aliasCount > 0 {
			defer q.Release(unsafe.Pointer(aliases))
			slice := charStarArrayToSlice(aliases, length)
			for i, s := range slice {
				output[i] = C.GoString(s)
			}
		}
		return output, nil
	}
	return nil, wrapError(err, "find_execute", "query", query)
}
