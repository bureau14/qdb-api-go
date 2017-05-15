package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/tag.h>
	#include <stdlib.h>
*/
import "C"
import "unsafe"

type entry struct {
	HandleType
	Expiry
	alias []byte
}

func (e entry) HasTag(tag []byte) error {
	err := C.qdb_has_tag(e.handle, (*C.char)(unsafe.Pointer(&e.alias[0])), (*C.char)(unsafe.Pointer(&tag[0])))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) AttachTag(tag []byte) error {
	err := C.qdb_attach_tag(e.handle, (*C.char)(unsafe.Pointer(&e.alias[0])), (*C.char)(unsafe.Pointer(&tag[0])))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) AttachTags(tags [][]byte) error {
	data := convertToCharStarStar(tags)
	defer C.free(data)
	err := C.qdb_attach_tags(e.handle, (*C.char)(unsafe.Pointer(&e.alias[0])), data, C.size_t(len(tags)))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) DetachTag(tag []byte) error {
	err := C.qdb_detach_tag(e.handle, (*C.char)(unsafe.Pointer(&e.alias[0])), (*C.char)(unsafe.Pointer(&tag[0])))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) DetachTags(tags [][]byte) error {
	data := convertToCharStarStar(tags)
	defer C.free(data)
	err := C.qdb_detach_tags(e.handle, (*C.char)(unsafe.Pointer(&e.alias[0])), data, C.size_t(len(tags)))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}
