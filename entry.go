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
	return e.alias
}

// Remove : entry remove value of alias
func (e entry) Remove() error {
	err := C.qdb_remove(e.handle, C.CString(e.alias))
	return makeErrorOrNil(err)
}

func (e entry) HasTag(tag string) error {
	err := C.qdb_has_tag(e.handle, C.CString(e.alias), C.CString(tag))
	return makeErrorOrNil(err)
}

func (e entry) AttachTag(tag string) error {
	err := C.qdb_attach_tag(e.handle, C.CString(e.alias), C.CString(tag))
	return makeErrorOrNil(err)
}

func (e entry) AttachTags(tags []string) error {
	data := convertToCharStarStar(tags)
	defer C.free(data)
	err := C.qdb_attach_tags(e.handle, C.CString(e.alias), data, C.size_t(len(tags)))
	return makeErrorOrNil(err)
}

func (e entry) DetachTag(tag string) error {
	err := C.qdb_detach_tag(e.handle, C.CString(e.alias), C.CString(tag))
	return makeErrorOrNil(err)
}

func (e entry) DetachTags(tags []string) error {
	data := convertToCharStarStar(tags)
	defer C.free(data)
	err := C.qdb_detach_tags(e.handle, C.CString(e.alias), data, C.size_t(len(tags)))
	return makeErrorOrNil(err)
}

func (e entry) GetTagged(tag string) ([]string, error) {
	var aliasCount C.size_t
	var aliases **C.char
	defer e.Release(unsafe.Pointer(aliases))

	err := C.qdb_get_tagged(e.handle, C.CString(tag), &aliases, &aliasCount)

	length := int(aliasCount)
	output := make([]string, length)
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(aliases))[:length:length]
	for i, s := range tmpslice {
		output[i] = C.GoString(s)
		defer e.Release(unsafe.Pointer(s))
	}

	return output, makeErrorOrNil(err)
}

func (e entry) GetTags() ([]string, error) {
	var tagCount C.size_t
	var tags **C.char
	defer e.Release(unsafe.Pointer(tags))
	err := C.qdb_get_tags(e.handle, C.CString(e.alias), &tags, &tagCount)

	length := int(tagCount)
	output := make([]string, length)
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(tags))[:length:length]
	for i, s := range tmpslice {
		output[i] = C.GoString(s)
		defer e.Release(unsafe.Pointer(s))
	}
	return output, makeErrorOrNil(err)
}
