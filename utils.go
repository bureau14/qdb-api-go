package qdb

/*
	#include <qdb/client.h>
*/
import "C"
import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"time"
	"unsafe"
)

func convertToCharStarStar(toConvert []string) unsafe.Pointer {
	ptrSize := unsafe.Sizeof(C.CString(toConvert[0]))
	size := len(toConvert)
	data := C.malloc(C.size_t(size) * C.size_t(ptrSize))
	for i := 0; i < size; i++ {
		element := (**C.char)(unsafe.Pointer(uintptr(data) + uintptr(i)*ptrSize))
		*element = (*C.char)(C.CString(toConvert[i]))
	}
	return data
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

// WriteJsonToFile : Write a json object to a file
func WriteJsonToFile(path string, jsonObject interface{}) error {
	data, err := json.MarshalIndent(&jsonObject, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, data, 0744)
}
