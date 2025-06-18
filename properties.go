package qdb

/*
	#include <qdb/properties.h>
	#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"strings"
)

var systemApiPropertyName = "api"
var systemApiName = "go"
var systemApiVersionPropertyName = "api_version"

func (h HandleType) PutProperties(prop string, value string) error {
	normalizedProperty, normalizeError := normalizeProperty(prop)
	if normalizeError != nil {
		return normalizeError
	}
	err := C.qdb_user_properties_put(h.handle, C.CString(normalizedProperty), C.CString(value))
	return makeErrorOrNil(err)
}

func (h HandleType) UpdateProperties(prop string, value string) error {
	normalizedProperty, normalizeError := normalizeProperty(prop)
	if normalizeError != nil {
		return normalizeError
	}
	if normalizedProperty != systemApiPropertyName && normalizedProperty != systemApiVersionPropertyName {
		err := C.qdb_user_properties_update(h.handle, C.CString(normalizedProperty), C.CString(value))
		return makeErrorOrNil(err)
	}
	return errors.New("could not overwrite system property")
}

func (h HandleType) GetProperties(prop string) (string, error) {
	normalizedProperty, normalizeError := normalizeProperty(prop)
	if normalizeError != nil {
		return "", normalizeError
	}
	var value *C.char
	defer releaseCharStar(value)
	err := C.qdb_user_properties_get(h.handle, C.CString(normalizedProperty), &value)
	return C.GoString(value), makeErrorOrNil(err)
}

func (h HandleType) RemoveProperties(prop string) error {
	normalizedProperty, normalizeError := normalizeProperty(prop)
	if normalizeError != nil {
		return normalizeError
	}
	if normalizedProperty != systemApiPropertyName && normalizedProperty != systemApiVersionPropertyName {
		err := C.qdb_user_properties_remove(h.handle, C.CString(prop))
		return makeErrorOrNil(err)
	}
	return errors.New("could not remove system property")
}

func (h HandleType) RemoveAllProperties() error {
	err := C.qdb_user_properties_remove_all(h.handle)

	propErr := h.putSystemProperties()
	if propErr != nil {
		return propErr
	}

	return makeErrorOrNil(err)
}

func normalizeProperty(prop string) (string, error) {
	normalizedProperty := prop
	if prop == "" {
		return prop, errors.New("property is empty")
	}
	normalizedProperty = strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(prop), "\n", ""), "\r", "")
	return normalizedProperty, nil
}

func (h HandleType) putSystemProperties() error {
	err := h.PutProperties(systemApiPropertyName, systemApiName)
	if err != nil {
		return err
	}
	err = h.PutProperties(systemApiVersionPropertyName, GitHash)
	if err != nil {
		return err
	}
	return nil
}
