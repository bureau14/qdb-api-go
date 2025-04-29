package qdb

/*
	#include <qdb/properties.h>
*/
import "C"
import (
	"errors"
	"strings"
)

var systemApiPropertyName = "api"
var systemApiVersionPropertyName = "api_version"

func (h HandleType) PutProperties(prop string, value string) error {
	normalizedProperty := normalizeProperty(prop)
	err := C.qdb_user_properties_put(h.handle, C.CString(normalizedProperty), C.CString(value))
	return ErrorType(err)
}

func (h HandleType) UpdateProperties(prop string, value string) error {
	normalizedProperty := normalizeProperty(prop)
	if normalizedProperty != systemApiPropertyName && normalizedProperty != systemApiVersionPropertyName {
		err := C.qdb_user_properties_update(h.handle, C.CString(normalizedProperty), C.CString(value))
		return ErrorType(err)
	}
	return errors.New("could not overwrite system property")
}

func (h HandleType) GetProperties(prop string) (string, error) {
	normalizedProperty := normalizeProperty(prop)
	var value *C.char
	defer releaseCharStar(value)
	err := C.qdb_user_properties_get(h.handle, C.CString(normalizedProperty), &value)
	return C.GoString(value), ErrorType(err)
}

func (h HandleType) RemoveProperties(prop string) error {
	normalizedProperty := normalizeProperty(prop)
	if normalizedProperty != systemApiPropertyName && normalizedProperty != systemApiVersionPropertyName {
		err := C.qdb_user_properties_remove(h.handle, C.CString(prop))
		return ErrorType(err)
	}
	return errors.New("could not remove system property")
}

func (h HandleType) RemoveAllProperties() error {
	err := C.qdb_user_properties_remove_all(h.handle)

	apiNameErr := h.PutProperties(systemApiPropertyName, "go")
	if apiNameErr != nil {
		return apiNameErr
	}
	apiVersionErr := h.PutProperties(systemApiVersionPropertyName, GitHash)
	if apiVersionErr != nil {
		return apiVersionErr
	}
	return ErrorType(err)
}

func normalizeProperty(prop string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(prop), "\n", ""), "\r", "")
}
