package qdb

/*
	#include <qdb/properties.h>
*/
import "C"
import "unsafe"

func (h HandleType) PutProperties(prop string, value string) error {
	err := C.qdb_user_properties_put(h.handle, C.CString(prop), C.CString(value))
	return ErrorType(err)
}

func (h HandleType) UpdateProperties(prop string, value string) error {
	err := C.qdb_user_properties_update(h.handle, C.CString(prop), C.CString(value))
	return ErrorType(err)
}

func (h HandleType) GetProperties(prop string) (string, error) {
	var value *C.char
	defer h.handle.Release(unsafe.Pointer(value))
	err := C.qdb_user_properties_get(h.handle, C.CString(prop), &value)
	return C.GoStringN(value), ErrorType(err)
}

func (h HandleType) RemoveProperties(prop string) error {
	err := C.qdb_user_properties_remove(h.handle, C.CString(prop))
	return ErrorType(err)
}

func (h HandleType) RemoveAllProperties() error {
	err := C.qdb_user_properties_remove_all(h.handle)
	return ErrorType(err)
}
