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

func generateIndex(n int, step time.Duration) []time.Time {
	var ret []time.Time = make([]time.Time, n)

	var start = time.Unix(1744512728, 0)

	for i, _ := range ret {
		nsec := step.Nanoseconds() * int64(i)
		ret[i] = start.Add(time.Duration(nsec))
	}

	return ret
}

func charStarArrayToSlice(strings **C.char, length int) []*C.char {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof((*C.char)(nil))]*C.char)(unsafe.Pointer(strings))[:length:length]
}

func TimeSliceToQdb
