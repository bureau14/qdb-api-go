package qdb

/*
	#include <qdb/client.h>
	#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"math"
	"math/rand"
	"time"
	"unsafe"
)

// All available columns we support
var columnTypes = [...]TsColumnType{TsColumnInt64, TsColumnDouble, TsColumnTimestamp}

func convertToCharStarStar(toConvert []string) unsafe.Pointer {
	var v *C.char
	ptrSize := unsafe.Sizeof(v)
	size := len(toConvert)
	data := C.malloc(C.size_t(size) * C.size_t(ptrSize))
	for i := 0; i < size; i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(data) + uintptr(i)*ptrSize))
		*element = (*C.char)(convertToCharStar(toConvert[i]))
	}
	return data
}

func releaseCharStarStar(data unsafe.Pointer, size int) {
	var v *C.char
	ptrSize := unsafe.Sizeof(v)
	for i := 0; i < size; i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(data) + uintptr(i)*ptrSize))
		releaseCharStar(*element)
	}
	C.free(data)
}

func convertToCharStar(toConvert string) *C.char {
	if len(toConvert) == 0 {
		return nil
	}
	return C.CString(toConvert)
}

func releaseCharStar(data *C.char) {
	if data != nil {
		C.free(unsafe.Pointer(data))
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func generateAlias(n int) string {
	nanos := int64(time.Now().UnixNano())
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	rand.Seed(nanos)
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// Returns a default-size alias (16 characters
func generateDefaultAlias() string {
	return generateAlias(16)
}

func generateColumnName() string {
	return generateAlias(16)
}

// Returns a random column type
func randomColumnType() TsColumnType {
	n := rand.Intn(len(columnTypes))

	return columnTypes[n]

}

// Generates names for exactly `n` column names
func generateColumnNames(n int) []string {
	var ret []string = make([]string, n)

	for i, _ := range ret {
		ret[i] = generateColumnName()
	}

	return ret
}

// Generate writer column info for exactly `n` columns.
func generateWriterColumns(n int) []WriterColumn {

	var ret []WriterColumn = make([]WriterColumn, n)

	for i, _ := range ret {
		cname := generateColumnName()
		ctype := randomColumnType()
		ret[i] = WriterColumn{cname, ctype}
	}

	return ret
}

func generateWriterColumnsOfAllTypes() []WriterColumn {
	// Generate column information for each available column type.
	var ret []WriterColumn = make([]WriterColumn, len(columnTypes))

	for i, ctype := range columnTypes {
		cname := generateColumnName()
		ret[i] = WriterColumn{cname, ctype}
	}

	return ret
}

// Similar to `generateWriterColumns`, but ensures all columns are of the specified
// type.
func generateWriterColumnsOfType(n int, ctype TsColumnType) []WriterColumn {

	// Lazy approach: just generate using random column types, then overwrite
	ret := generateWriterColumns(n)

	for i, _ := range ret {
		ret[i].ColumnType = ctype
	}

	return ret
}

func generateWriterDataInt64(n int) WriterData {
	xs := make([]int64, n)

	for i, _ := range xs {
		xs[i] = rand.Int63()
	}

	return NewWriterDataInt64(&xs)
}

func generateWriterDataDouble(n int) WriterData {
	xs := make([]float64, n)

	for i, _ := range xs {
		xs[i] = rand.NormFloat64()
	}

	return NewWriterDataDouble(&xs)
}

func generateWriterDataTimestamp(n int) WriterData {
	// XXX(leon): should be improved to be more random, instead
	//            we're reusing the code that generates the index here.

	xs := make([]C.qdb_timespec_t, n)

	idx := generateDefaultIndex(n)

	for i, t := range *idx {
		xs[i] = TimeToQdbTimespec(t)
	}

	return NewWriterDataTimestamp(&xs)
}

// Generates artifical writer data for a single column
func generateWriterData(n int, column WriterColumn) WriterData {
	switch column.ColumnType {
	case TsColumnInt64:
		return generateWriterDataInt64(n)
	case TsColumnDouble:
		return generateWriterDataDouble(n)
	case TsColumnTimestamp:
		return generateWriterDataTimestamp(n)
	}

	panic(fmt.Sprintf("Unrecognized column type: %v", column.ColumnType))
}

// Generates artificial data to be inserted for each column.
func generateWriterDatas(n int, columns []WriterColumn) []WriterData {
	var ret []WriterData = make([]WriterData, len(columns))

	for i, column := range columns {
		ret[i] = generateWriterData(n, column)
	}

	return ret
}

// Generates an time index
func generateIndex(n int, start time.Time, step time.Duration) *[]time.Time {
	var ret []time.Time = make([]time.Time, n)

	for i, _ := range ret {
		nsec := step.Nanoseconds() * int64(i)
		ret[i] = start.Add(time.Duration(nsec))
	}

	return &ret
}

// Generates an index with a default start date and step
func generateDefaultIndex(n int) *[]time.Time {

	var start time.Time = time.Unix(1745514000, 0) // 2025-04-25
	var duration time.Duration = 100 * 1000 * 1000 // 100ms

	return generateIndex(n, start, duration)
}

func charStarArrayToSlice(strings **C.char, length int) []*C.char {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof((*C.char)(nil))]*C.char)(unsafe.Pointer(strings))[:length:length]
}
