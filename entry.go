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

// ::: EXPIRY RELATED FUNCTIONS :::

// Expiry : expiration value
type Expiry TimeType

const (
	// NeverExpires : constant value for unexpirable data
	NeverExpires = C.qdb_never_expires
	// PreserveExpiration : constant value for preservation of expiration value
	PreserveExpiration = C.qdb_preserve_expiration
)

// ExpiresAt : Sets the absolute expiration time of an entry, if the type supports expiration.
// Blobs and integers can have an expiration time and will be automatically removed by the cluster when they expire.
// The absolute expiration time is the Unix epoch, that is, the number of milliseconds since 1 January 1970, 00:00::00 UTC.
// To use a relative expiration time (that is expiration relative to the time of the call), use ExpiresFromNow.
// To remove the expiration time of an entry, specify the value qdb_never_expires as expiry_time parameter.
// Values in the past are refused, but the cluster will have a certain tolerance to account for clock skews.
func (e entry) ExpiresAt(expiry TimeType) error {
	err := C.qdb_expires_at(e.handle, C.CString(e.alias), C.qdb_time_t(expiry))
	return makeErrorOrNil(err)
}

// ExpiresFromNow : Sets the expiration time of an entry, relative to the current time of the client, if the type supports expiration.
// Blobs and integers can have an expiration time and will automatically be removed by the cluster when they expire.
// The expiration is relative to the current time of the machine.
// To remove the expiration time of an entry or to use an absolute expiration time use ExpiresAt.
func (e entry) ExpiresFromNow(expiryDelta TimeType) error {
	err := C.qdb_expires_from_now(e.handle, C.CString(e.alias), C.qdb_time_t(expiryDelta))
	return makeErrorOrNil(err)
}

// ::: END OF EXPIRY RELATED FUNCTIONS :::

// NodeLocation : A structure representing the address of a quasardb node.
type NodeLocation struct {
	Address string
	Port    int16
}

// GetLocation : Returns the primary node of an entry.
// The exact location of an entry should be assumed random and users should not bother about its location as the API will transparently locate the best node for the requested operation.
// This function is intended for higher level APIs that need to optimize transfers and potentially push computation close to the data.
// This function allocates memory for the null terminated address string call qdb_release on the location structure to release memory.
func (e entry) GetLocation() (NodeLocation, error) {
	var location C.qdb_remote_node_t
	defer e.Release(unsafe.Pointer(&location))
	err := C.qdb_get_location(e.handle, C.CString(e.alias), &location)
	return NodeLocation{C.GoString(location.address), int16(location.port)}, makeErrorOrNil(err)
}

// RefID : Unique identifier
type RefID C.qdb_id_t

// EntryType : An enumeration representing possible entries type.
type EntryType C.qdb_entry_type_t

const (
	// EntryUnitialized : Uninitialized value.
	EntryUnitialized = C.qdb_entry_uninitialized
	// EntryBlob : A binary large object (blob).
	EntryBlob = C.qdb_entry_blob
	// EntryInteger : A signed 64-bit integer.
	EntryInteger = C.qdb_entry_integer
	// EntryHSet : A distributed hash set.
	EntryHSet = C.qdb_entry_hset
	// EntryTag : A tag.
	EntryTag = C.qdb_entry_tag
	// EntryDeque : A distributed double-entry queue (deque).
	EntryDeque = C.qdb_entry_deque
	// EntryStream : A distributed binary stream.
	EntryStream = C.qdb_entry_stream
	// EntryTS : A distributed time series.
	EntryTS = C.qdb_entry_ts
)

// Metadata : A structure representing the metadata of an entry in the database.
type Metadata struct {
	Ref              RefID
	Type             EntryType
	Size             uint64
	ModificationTime TimespecType
	ExpiryTime       TimespecType
}

func (e entry) GetMetadata() (Metadata, error) {
	var m C.qdb_entry_metadata_t
	err := C.qdb_get_metadata(e.handle, C.CString(e.alias), &m)
	return Metadata{RefID(m.reference), EntryType(m._type), uint64(m.size), m.modification_time.toTimeSpec(), m.expiry_time.toTimeSpec()}, makeErrorOrNil(err)
}

// ::: TAGS RELATED FUNCTIONS :::

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
	err := C.qdb_get_tagged(e.handle, C.CString(tag), &aliases, &aliasCount)

	if err == 0 {
		defer e.Release(unsafe.Pointer(aliases))
		length := int(aliasCount)
		output := make([]string, length)
		tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(aliases))[:length:length]
		for i, s := range tmpslice {
			output[i] = C.GoString(s)
		}
		return output, nil
	}
	return nil, ErrorType(err)
}

func (e entry) GetTags() ([]string, error) {
	var tagCount C.size_t
	var tags **C.char
	err := C.qdb_get_tags(e.handle, C.CString(e.alias), &tags, &tagCount)

	if err == 0 {
		defer e.Release(unsafe.Pointer(tags))
		length := int(tagCount)
		output := make([]string, length)
		if length > 0 {
			tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(tags))[:length:length]
			for i, s := range tmpslice {
				output[i] = C.GoString(s)
			}
		}
		return output, nil
	}
	return nil, ErrorType(err)
}
