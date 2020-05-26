// Package qdb provides an api to a quasardb server
package qdb

/*
	#include <stdlib.h>
	#include <qdb/node.h>
	#include <qdb/tag.h>
	#include <qdb/ts.h>
	#include <qdb/prefix.h>
*/
import "C"
import (
	"encoding/json"
	"io/ioutil"
	"time"
	"unsafe"
)

// HandleType : An opaque handle to internal API-allocated structures needed for maintaining connection to a cluster.
type HandleType struct {
	handle C.qdb_handle_t
}

// Protocol : A network protocol.
type Protocol C.qdb_protocol_t

// Protocol values:
//	ProtocolTCP : Uses TCP/IP to communicate with the cluster. This is currently the only supported network protocol.
const (
	ProtocolTCP Protocol = C.qdb_p_tcp
)

// Compression : compression parameter
type Compression C.qdb_compression_t

// Compression values:
//	CompNone : No compression.
//	CompFast : Maximum compression speed, potentially minimum compression ratio. This is currently the default.
//	CompBest : Maximum compression ratio, potentially minimum compression speed. This is currently not implemented.
const (
	CompNone Compression = C.qdb_comp_none
	CompFast Compression = C.qdb_comp_fast
	CompBest Compression = C.qdb_comp_best
)

// APIVersion : Returns a string describing the API version.
func (h HandleType) APIVersion() string {
	version := C.qdb_version()
	defer h.Release(unsafe.Pointer(version))
	return C.GoString(version)
}

// APIBuild : Returns a string describing the exact API build.
func (h HandleType) APIBuild() string {
	build := C.qdb_build()
	defer h.Release(unsafe.Pointer(build))
	return C.GoString(build)
}

// Open : Creates a handle.
//	No connection will be established.
//	Not needed if you created your handle with NewHandle.
func (h HandleType) Open(protocol Protocol) error {
	err := C.qdb_open(&h.handle, C.qdb_protocol_t(protocol))
	return makeErrorOrNil(err)
}

// SetTimeout : Sets the timeout of all network operations.
//	The lower the timeout, the higher the risk of having timeout errors.
//	Keep in mind that the server-side timeout might be shorter.
func (h HandleType) SetTimeout(timeout time.Duration) error {
	err := C.qdb_option_set_timeout(h.handle, C.int(timeout/time.Millisecond))
	return makeErrorOrNil(err)
}

// Encryption : encryption option
type Encryption C.qdb_encryption_t

// Encryption values:
//	EncryptNone : No encryption.
//	EncryptAES : Uses aes gcm 256 encryption.
const (
	EncryptNone Encryption = C.qdb_crypt_none
	EncryptAES  Encryption = C.qdb_crypt_aes_gcm_256
)

// SetEncryption : Creates a handle.
//	No connection will be established.
//	Not needed if you created your handle with NewHandle.
func (h HandleType) SetEncryption(encryption Encryption) error {
	err := C.qdb_option_set_encryption(h.handle, C.qdb_encryption_t(encryption))
	return makeErrorOrNil(err)
}

type jSONCredentialConfig struct {
	Username  string `json:"username"`
	SecretKey string `json:"secret_key"`
}

// UserCredentialFromFile : retrieve user credentials from a file
func UserCredentialFromFile(userCredentialFile string) (string, string, error) {
	fileConfig, err := ioutil.ReadFile(userCredentialFile)
	if err != nil {
		return "", "", err
	}
	var jsonConfig jSONCredentialConfig
	err = json.Unmarshal(fileConfig, &jsonConfig)
	if err != nil {
		return "", "", err
	}
	return jsonConfig.Username, jsonConfig.SecretKey, nil
}

// ClusterKeyFromFile : retrieve cluster key from a file
func ClusterKeyFromFile(clusterPublicKeyFile string) (string, error) {
	clusterPublicKey, err := ioutil.ReadFile(clusterPublicKeyFile)
	if err != nil {
		return "", err
	}
	return string(clusterPublicKey), nil
}

// AddUserCredentials : add a username and key from a user name and secret.
func (h HandleType) AddUserCredentials(name, secret string) error {
	username := convertToCharStar(name)
	defer releaseCharStar(username)
	userSecret := convertToCharStar(secret)
	defer releaseCharStar(userSecret)
	qdbErr := C.qdb_option_set_user_credentials(h.handle, username, userSecret)
	return makeErrorOrNil(qdbErr)
}

// AddClusterPublicKey : add the cluster public key from a cluster config file.
func (h HandleType) AddClusterPublicKey(secret string) error {
	clusterPublicKey := convertToCharStar(secret)
	defer releaseCharStar(clusterPublicKey)
	qdbErr := C.qdb_option_set_cluster_public_key(h.handle, clusterPublicKey)
	return makeErrorOrNil(qdbErr)
}

// SetMaxCardinality : Sets the maximum allowed cardinality of a quasardb query.
//	The default value is 10,007. The minimum allowed values is 100.
func (h HandleType) SetMaxCardinality(maxCardinality uint) error {
	err := C.qdb_option_set_max_cardinality(h.handle, C.qdb_uint_t(maxCardinality))
	return makeErrorOrNil(err)
}

// SetCompression : Set the compression level for all future messages emitted by the specified handle.
//	Regardless of this parameter, the API will be able to read whatever compression the server uses.
func (h HandleType) SetCompression(compressionLevel Compression) error {
	err := C.qdb_option_set_compression(h.handle, C.qdb_compression_t(compressionLevel))
	return makeErrorOrNil(err)
}

// SetClientMaxInBufSize : Set the Sets the maximum incoming buffer size for all network operations of the client.
//  Only modify this setting if you expect to receive very large answers from the server.
func (h HandleType) SetClientMaxInBufSize(bufSize uint) error {
	err := C.qdb_option_set_client_max_in_buf_size(h.handle, C.size_t(bufSize))
	return makeErrorOrNil(err)
}

// GetClientMaxInBufSize : Gets the maximum incoming buffer size for all network operations of the client.
func (h HandleType) GetClientMaxInBufSize() (uint, error) {
	var bufSize C.size_t
	err := C.qdb_option_get_client_max_in_buf_size(h.handle, &bufSize)
	return uint(bufSize), makeErrorOrNil(err)
}

// GetClusterMaxInBufSize : Gets the maximum incoming buffer size for all network operations of the client.
func (h HandleType) GetClusterMaxInBufSize() (uint, error) {
	var bufSize C.size_t
	err := C.qdb_option_get_cluster_max_in_buf_size(h.handle, &bufSize)
	return uint(bufSize), makeErrorOrNil(err)
}

// Connect : connect a previously opened handle
//	Binds the client instance to a quasardb cluster and connect to at least one node within.
//	Quasardb URI are in the form qdb://<address>:<port> where <address> is either an IPv4 or IPv6 (surrounded with square brackets), or a domain name. It is recommended to specify multiple addresses should the designated node be unavailable.
//
//	URI examples:
//		qdb://myserver.org:2836 - Connects to myserver.org on the port 2836
//		qdb://127.0.0.1:2836 - Connects to the local IPv4 loopback on the port 2836
//		qdb://myserver1.org:2836,myserver2.org:2836 - Connects to myserver1.org or myserver2.org on the port 2836
//		qdb://[::1]:2836 - Connects to the local IPv6 loopback on the port 2836
func (h HandleType) Connect(clusterURI string) error {
	uri := convertToCharStar(clusterURI)
	defer releaseCharStar(uri)
	err := C.qdb_connect(h.handle, uri)
	return makeErrorOrNil(err)
}

// Close : Closes the handle previously opened.
//	This results in terminating all connections and releasing all internal buffers,
//	including buffers which may have been allocated as or a result of batch operations or get operations.
func (h HandleType) Close() error {
	err := C.qdb_close(h.handle)
	return makeErrorOrNil(err)
}

// Release : Releases an API-allocated buffer.
//	Failure to properly call this function may result in excessive memory usage.
//	Most operations that return a content (e.g. batch operations, qdb_blob_get, qdb_blob_get_and_update, qdb_blob_compare_and_swap...)
//	will allocate a buffer for the content and will not release the allocated buffer until you either call this function or close the handle.
//
//	The function will be able to release any kind of buffer allocated by a quasardb API call, whether it’s a single buffer, an array or an array of buffers.
func (h HandleType) Release(buffer unsafe.Pointer) {
	C.qdb_release(h.handle, buffer)
}

// GetTags : Retrieves all the tags of an entry.
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The entry must exist.
func (h HandleType) GetTags(entryAlias string) ([]string, error) {
	alias := convertToCharStar(entryAlias)
	defer releaseCharStar(alias)
	var tagCount C.size_t
	var tags **C.char
	err := C.qdb_get_tags(h.handle, alias, &tags, &tagCount)

	if err == 0 {
		defer h.Release(unsafe.Pointer(tags))
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

// GetTagged : Retrieves all entries that have the specified tag.
//	Tagging an entry enables you to search for entries based on their tags. Tags scale across nodes.
//	The tag must exist.
//	The complexity of this function is constant.
func (h HandleType) GetTagged(tag string) ([]string, error) {
	cTag := convertToCharStar(tag)
	defer releaseCharStar(cTag)
	var aliasCount C.size_t
	var aliases **C.char
	err := C.qdb_get_tagged(h.handle, cTag, &aliases, &aliasCount)

	if err == 0 {
		defer h.Release(unsafe.Pointer(aliases))
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

// PrefixGet : Retrieves the list of all entries matching the provided prefix.
//	A prefix-based search will enable you to find all entries matching a provided prefix.
//	This function returns the list of aliases. It’s up to the user to query the content associated with every entry, if needed.
func (h HandleType) PrefixGet(prefix string, limit int) ([]string, error) {
	cPrefix := convertToCharStar(prefix)
	defer releaseCharStar(cPrefix)
	var entryCount C.size_t
	var entries **C.char
	err := C.qdb_prefix_get(h.handle, cPrefix, C.qdb_int_t(limit), &entries, &entryCount)

	if err == 0 {
		defer h.Release(unsafe.Pointer(entries))
		length := int(entryCount)
		output := make([]string, length)
		if length > 0 {
			slice := charStarArrayToSlice(entries, length)
			for i, s := range slice {
				output[i] = C.GoString(s)
			}
		}
		return output, nil
	}
	return []string{}, ErrorType(err)
}

// PrefixCount : Retrieves the count of all entries matching the provided prefix.
//	A prefix-based count counts all entries matching a provided prefix.
func (h HandleType) PrefixCount(prefix string) (uint64, error) {
	cPrefix := convertToCharStar(prefix)
	defer releaseCharStar(cPrefix)
	var count C.qdb_uint_t
	err := C.qdb_prefix_count(h.handle, cPrefix, &count)

	return uint64(count), makeErrorOrNil(err)
}

// Handles Creators

// NewHandle : Create a new handle, return error if needed
//	The handle is already opened (not connected) with tcp protocol
func NewHandle() (HandleType, error) {
	var h HandleType
	err := C.qdb_open((*C.qdb_handle_t)(&h.handle), C.qdb_protocol_t(ProtocolTCP))
	return h, makeErrorOrNil(err)
}

// SetupHandle : Setup a handle, return error if needed
//	The handle is already opened with tcp protocol
//	The handle is already connected with the clusterURI string
func SetupHandle(clusterURI string, timeout time.Duration) (HandleType, error) {
	h, err := NewHandle()
	if err != nil {
		return h, err
	}
	err = h.SetTimeout(timeout)
	if err != nil {
		return h, err
	}
	err = h.Connect(clusterURI)
	return h, err
}

// MustSetupHandle : Setup a handle, panic on error
//	The handle is already opened with tcp protocol
//	The handle is already connected with the clusterURI string
//
//	Panic on error
func MustSetupHandle(clusterURI string, timeout time.Duration) HandleType {
	h, err := SetupHandle(clusterURI, timeout)
	if err != nil {
		panic(err)
	}
	return h
}

// SetupSecuredHandle : Setup a secured handle, return error if needed
//	The handle is already opened with tcp protocol
//	The handle is already secured with the cluster public key and the user credential files provided
//	(Note: the filenames are needed, not the content of the files)
//	The handle is already connected with the clusterURI string
func SetupSecuredHandle(clusterURI, clusterPublicKeyFile, userCredentialFile string, timeout time.Duration, encryption Encryption) (HandleType, error) {
	h, err := NewHandle()
	if err != nil {
		return h, err
	}
	clusterKey, err := ClusterKeyFromFile(clusterPublicKeyFile)
	if err != nil {
		return h, err
	}
	err = h.AddClusterPublicKey(clusterKey)
	if err != nil {
		return h, err
	}
	user, secret, err := UserCredentialFromFile(userCredentialFile)
	if err != nil {
		return h, err
	}
	err = h.AddUserCredentials(user, secret)
	if err != nil {
		return h, err
	}
	err = h.SetTimeout(timeout)
	if err != nil {
		return h, err
	}
	err = h.SetEncryption(encryption)
	if err != nil {
		return h, err
	}
	err = h.Connect(clusterURI)
	return h, err
}

// MustSetupSecuredHandle : Setup a secured handle, panic on error
//	The handle is already opened with tcp protocol
//	The handle is already secured with the cluster public key and the user credential files provided
//	(Note: the filenames are needed, not the content of the files)
//	The handle is already connected with the clusterURI string
func MustSetupSecuredHandle(clusterURI, clusterPublicKeyFile, userCredentialFile string, timeout time.Duration, encryption Encryption) HandleType {
	h, err := SetupSecuredHandle(clusterURI, clusterPublicKeyFile, userCredentialFile, timeout, encryption)
	if err != nil {
		panic(err)
	}
	return h
}

// Entries creators

// Blob : Create a blob entry object
func (h HandleType) Blob(alias string) BlobEntry {
	return BlobEntry{Entry{h, alias}}
}

// Integer : Create an integer entry object
func (h HandleType) Integer(alias string) IntegerEntry {
	return IntegerEntry{Entry{h, alias}}
}

// Timeseries : Create a timeseries entry object
func (h HandleType) Timeseries(alias string) TimeseriesEntry {
	return TimeseriesEntry{Entry{h, alias}}
}

// Node : Create a node object
func (h HandleType) Node(uri string) *Node {
	return &Node{h, uri}
}

// Find : Create a query object to execute
func (h HandleType) Find() *Find {
	return &Find{h, []string{}, []string{}, []string{}}
}

// Cluster : Create a cluster object to execute commands on a cluster
func (h HandleType) Cluster() *Cluster {
	return &Cluster{h}
}

// Query : Create an query object to execute
func (h HandleType) Query(query string) *Query {
	return &Query{h, query}
}

// TsBatch : create a batch object for the specified columns
func (h HandleType) TsBatch(cols ...TsBatchColumnInfo) (*TsBatch, error) {
	columns := tsBatchColumnInfoArrayToC(cols...)
	defer releaseTsBatchColumnInfoArray(columns, len(cols))
	columnsCount := C.qdb_size_t(len(cols))
	batch := &TsBatch{}
	batch.h = h
	err := C.qdb_ts_batch_table_init(h.handle, columns, columnsCount, &batch.table)
	return batch, makeErrorOrNil(err)
}

func (h HandleType) GetLastError() (string, error) {
	var err C.qdb_error_t
	var message C.qdb_string_t
	C.qdb_get_last_error(h.handle, &err, &message)
	return C.GoString(message.data), makeErrorOrNil(err)
}
