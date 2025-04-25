package qdb

/*
	#include <qdb/client.h>
	#include <stdlib.h>
*/
import "C"
import (
	"math"
	"math/rand"
	"time"
	"unsafe"
)

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

func generateColumnName() string {
	return generateAlias(16)
}

// Generates names for exactly `n` column names
func generateColumnNames(n int) []string {
	var ret []string = make([]string, n)

	for i, _ := range ret {
		ret[i] = generateColumnName()
	}

	return ret
}

// Generates an time index
func generateIndex(n int, start time.Time, step time.Duration) []time.Time {
	var ret []time.Time = make([]time.Time, n)

	for i, _ := range ret {
		nsec := step.Nanoseconds() * int64(i)
		ret[i] = start.Add(time.Duration(nsec))
	}

	return ret
}

// Generates an index with a default start date and step
func generateDefaultIndex(n int) []time.Time {

	var start time.Time = time.Unix(1745514000, 0) // 2025-04-25
	var duration time.Duration = 100 * 1000 * 1000 // 100ms

	return generateIndex(n, start, duration)
}

func charStarArrayToSlice(strings **C.char, length int) []*C.char {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof((*C.char)(nil))]*C.char)(unsafe.Pointer(strings))[:length:length]
}

// Converts a single time.Time value to a native C qdb_timespec_t value
func timeToQdbTimespec(t time.Time) C.qdb_timespec_t {
	nsec := C.qdb_time_t(t.Nanosecond())
	sec := C.qdb_time_t(t.Unix())

	return C.qdb_timespec_t{sec, nsec}
}

// Converts a slice of `time.Time` values to a slice of native C qdb_timespec_t values
func timeSliceToQdbTimespec(xs []time.Time) []C.qdb_timespec_t {
	ret := make([]C.qdb_timespec_t, len(xs))

	for i := range xs {
		ret[i] = timeToQdbTimespec(xs[i])
	}

	return ret
}
