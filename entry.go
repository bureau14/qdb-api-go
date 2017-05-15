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
	alias string
}

func (e entry) Alias() string {
	return string(e.alias)
}

func (e entry) HasTag(tag []string) error {
	err := C.qdb_has_tag(e.handle, C.CString(e.alias), (*C.char)(unsafe.Pointer(&tag[0])))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) AttachTag(tag string) error {
	err := C.qdb_attach_tag(e.handle, C.CString(e.alias), C.CString(tag))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) AttachTags(tags []string) error {
	data := convertToCharStarStar(tags)
	defer C.free(data)
	err := C.qdb_attach_tags(e.handle, C.CString(e.alias), data, C.size_t(len(tags)))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) DetachTag(tag string) error {
	err := C.qdb_detach_tag(e.handle, C.CString(e.alias), C.CString(tag))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) DetachTags(tags []string) error {
	data := convertToCharStarStar(tags)
	defer C.free(data)
	err := C.qdb_detach_tags(e.handle, C.CString(e.alias), data, C.size_t(len(tags)))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}
