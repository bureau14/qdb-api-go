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
	alias string
}

// Alias : Return alias value
func (e entry) Alias() string {
	return string(e.alias)
}

// Remove : entry remove value of alias
func (e entry) Remove() error {
	err := C.qdb_remove(e.handle, C.CString(e.alias))
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}

func (e entry) HasTag(tag string) error {
	err := C.qdb_has_tag(e.handle, C.CString(e.alias), C.CString(tag))
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

func (e entry) GetTagged(tag string) ([]string, error) {
	var aliases **C.char
	var aliasCount C.size_t
	err := C.qdb_get_tagged(e.handle, C.CString(tag), &aliases, &aliasCount)
	if err != 0 {
		return nil, ErrorType(err)
	}
	length := int(aliasCount)
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(aliases))[:length:length]
	output := make([]string, length)
	for i, s := range tmpslice {
		output[i] = C.GoString(s)
		e.Release(unsafe.Pointer(s))
	}
	e.Release(unsafe.Pointer(aliases))
	return output, nil
}

func (e entry) GetTags() ([]string, error) {
	var tags **C.char
	var tagCount C.size_t
	err := C.qdb_get_tags(e.handle, C.CString(e.alias), &tags, &tagCount)
	if err != 0 {
		return nil, ErrorType(err)
	}
	length := int(tagCount)
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(tags))[:length:length]
	output := make([]string, length)
	for i, s := range tmpslice {
		output[i] = C.GoString(s)
		e.Release(unsafe.Pointer(s))
	}
	e.Release(unsafe.Pointer(tags))
	return output, nil
}
