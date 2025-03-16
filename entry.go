package qdb

/*
	#include <qdb/tag.h>
	#include <stdlib.h>
*/
import "C"
import (
	"time"
	"unsafe"
)

// Entry : cannot be constructed
// base type for composition
type Entry struct {
	HandleType
	alias string
}

// Alias : Return an alias string of the object
func (e Entry) Alias() string {
	return e.alias
}

// Remove : Removes an entry from the cluster, regardless of its type.
//
//	This call will remove the entry, whether it is a blob, integer, deque, stream.
//	It will properly untag the entry.
//	If the entry spawns on multiple entries or nodes (deques and streams) all blocks will be properly removed.
//
//	The call is ACID, regardless of the type of the entry and a transaction will be created if need be
func (e Entry) Remove() error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	err := C.qdb_remove(e.handle, alias)
	return makeErrorOrNil(err)
}

// ::: EXPIRY RELATED FUNCTIONS :::

// ExpiresAt : Sets the absolute expiration time of an entry.
//
//	Blobs and integers can have an expiration time and will be automatically removed by the cluster when they expire.
//
//	The absolute expiration time is the Unix epoch, that is, the number of milliseconds since 1 January 1970, 00:00::00 UTC.
//	To use a relative expiration time (that is expiration relative to the time of the call), use ExpiresFromNow.
//
//	To remove the expiration time of an entry, specify the value NeverExpires as ExpiryTime parameter.
//	Values in the past are refused, but the cluster will have a certain tolerance to account for clock skews.
func (e Entry) ExpiresAt(expiry time.Time) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	err := C.qdb_expires_at(e.handle, alias, toQdbTime(expiry))
	return makeErrorOrNil(err)
}

// ExpiresFromNow : Sets the expiration time of an entry, relative to the current time of the client.
//
//	Blobs and integers can have an expiration time and will automatically be removed by the cluster when they expire.
//
//	The expiration is relative to the current time of the machine.
//	To remove the expiration time of an entry or to use an absolute expiration time use ExpiresAt.
func (e Entry) ExpiresFromNow(expiry time.Duration) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	err := C.qdb_expires_from_now(e.handle, alias, C.qdb_time_t(expiry/time.Millisecond))
	return makeErrorOrNil(err)
}

// ::: END OF EXPIRY RELATED FUNCTIONS :::

// NodeLocation : A structure representing the address of a quasardb node.
type NodeLocation struct {
	Address string
	Port    int16
}

// GetLocation : Returns the primary node of an entry.
//
//	The exact location of an entry should be assumed random and users should not bother about its location as the API will transparently locate the best node for the requested operation.
//	This function is intended for higher level APIs that need to optimize transfers and potentially push computation close to the data.
func (e Entry) GetLocation() (NodeLocation, error) {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	var location C.qdb_remote_node_t
	defer e.Release(unsafe.Pointer(&location))
	err := C.qdb_get_location(e.handle, alias, &location)
	return NodeLocation{C.GoString(location.address), int16(location.port)}, makeErrorOrNil(err)
}

// RefID : Unique identifier
type RefID C.qdb_id_t

// EntryType : An enumeration representing possible entries type.
type EntryType C.qdb_entry_type_t

// EntryType Values
//
//	EntryUninitialized : Uninitialized value.
//	EntryBlob : A binary large object (blob).
//	EntryInteger : A signed 64-bit integer.
//	EntryHSet : A distributed hash set.
//	EntryTag : A tag.
//	EntryDeque : A distributed double-entry queue (deque).
//	EntryTS : A distributed time series.
//	EntryStream : A distributed binary stream.
const (
	EntryUninitialized EntryType = C.qdb_entry_uninitialized
	EntryBlob          EntryType = C.qdb_entry_blob
	EntryInteger       EntryType = C.qdb_entry_integer
	EntryHSet          EntryType = C.qdb_entry_hset
	EntryTag           EntryType = C.qdb_entry_tag
	EntryDeque         EntryType = C.qdb_entry_deque
	EntryStream        EntryType = C.qdb_entry_stream
	EntryTS            EntryType = C.qdb_entry_ts
)

// Metadata : A structure representing the metadata of an entry in the database.
type Metadata struct {
	Ref              RefID
	Type             EntryType
	Size             uint64
	ModificationTime time.Time
	ExpiryTime       time.Time
}

// GetMetadata : Gets the meta-information about an entry, if it exists.
func (e Entry) GetMetadata() (Metadata, error) {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	var m C.qdb_entry_metadata_t
	err := C.qdb_get_metadata(e.handle, alias, &m)
	return Metadata{RefID(m.reference), EntryType(m._type), uint64(m.size), TimespecToStructG(m.modification_time), TimespecToStructG(m.expiry_time)}, makeErrorOrNil(err)
}

// ::: TAGS RELATED FUNCTIONS :::

// AttachTag : Adds a tag entry.
//
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The entry must exist.
//	The tag may or may not exist.
func (e Entry) AttachTag(tag string) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	cTag := convertToCharStar(tag)
	defer releaseCharStar(cTag)
	err := C.qdb_attach_tag(e.handle, alias, cTag)
	return makeErrorOrNil(err)
}

// AttachTags : Adds a collection of tags to a single entry.
//
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The function will ignore existing tags.
//	The entry must exist.
//	The tag may or may not exist.
func (e Entry) AttachTags(tags []string) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	data := convertToCharStarStar(tags)
	defer releaseCharStarStar(data, len(tags))
	err := C.qdb_attach_tags(e.handle, alias, (**C.char)(data), C.size_t(len(tags)))
	return makeErrorOrNil(err)
}

// HasTag : Tests if an entry has the request tag.
//
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The entry must exist.
func (e Entry) HasTag(tag string) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	cTag := convertToCharStar(tag)
	defer releaseCharStar(cTag)
	err := C.qdb_has_tag(e.handle, alias, cTag)
	return makeErrorOrNil(err)
}

// DetachTag : Removes a tag from an entry.
//
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The entry must exist.
//	The tag must exist.
func (e Entry) DetachTag(tag string) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	cTag := convertToCharStar(tag)
	defer releaseCharStar(cTag)
	err := C.qdb_detach_tag(e.handle, alias, cTag)
	return makeErrorOrNil(err)
}

// DetachTags : Removes a collection of tags from a single entry.
//
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The entry must exist.
//	The tags must exist.
func (e Entry) DetachTags(tags []string) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	data := convertToCharStarStar(tags)
	defer releaseCharStarStar(data, len(tags))
	err := C.qdb_detach_tags(e.handle, alias, (**C.char)(data), C.size_t(len(tags)))
	return makeErrorOrNil(err)
}

// GetTagged : Retrieves all entries that have the specified tag.
//
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The tag must exist.
//	The complexity of this function is constant.
func (e Entry) GetTagged(tag string) ([]string, error) {
	cTag := convertToCharStar(tag)
	defer releaseCharStar(cTag)
	var aliasCount C.size_t
	var aliases **C.char
	err := C.qdb_get_tagged(e.handle, cTag, &aliases, &aliasCount)

	if err == 0 {
		defer e.Release(unsafe.Pointer(aliases))
		length := int(aliasCount)
		output := make([]string, length)
		if length > 0 {
			slice := charStarArrayToSlice(aliases, length)
			for i, s := range slice {
				output[i] = C.GoString(s)
			}
		}
		return output, nil
	}
	return nil, ErrorType(err)
}

// GetTags : Retrieves all the tags of an entry.
//
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The entry must exist.
func (e Entry) GetTags() ([]string, error) {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	var tagCount C.size_t
	var tags **C.char
	err := C.qdb_get_tags(e.handle, alias, &tags, &tagCount)

	if err == 0 {
		defer e.Release(unsafe.Pointer(tags))
		length := int(tagCount)
		output := make([]string, length)
		if length > 0 {
			slice := charStarArrayToSlice(tags, length)
			for i, s := range slice {
				output[i] = C.GoString(s)
			}
		}
		return output, nil
	}
	return nil, ErrorType(err)
}
