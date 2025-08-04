// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
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
	"fmt"
	"os"
	"time"
	"unsafe"
)

// HandleType is an opaque handle to internal API-allocated structures needed for maintaining connection to a cluster.
type HandleType struct {
	handle C.qdb_handle_t
}

// Protocol is a network protocol.
type Protocol C.qdb_protocol_t

// Protocol values:
//
//	ProtocolTCP : Uses TCP/IP to communicate with the cluster. This is currently the only supported network protocol.
const (
	ProtocolTCP Protocol = C.qdb_p_tcp
)

// Compression is a compression parameter.
type Compression C.qdb_compression_t

// Compression values:
//
//	CompNone : No compression.
//	CompBalanced : Balanced compression for speed and efficiency, recommended value.
const (
	CompNone     Compression = C.qdb_comp_none
	CompBalanced Compression = C.qdb_comp_balanced
)

// APIVersion returns the QuasarDB API version string.
//
// Args:
//
//	None
//
// Returns:
//
//	string: API version (e.g. "3.14.0")
//
// Example:
//
//	version := h.APIVersion() // → "3.14.0"
func (h HandleType) APIVersion() string {
	version := C.qdb_version()
	defer h.Release(unsafe.Pointer(version))
	return C.GoString(version)
}

// APIBuild returns the QuasarDB API build information.
//
// Args:
//
//	None
//
// Returns:
//
//	string: Build information including version, compiler, and platform
//
// Example:
//
//	build := h.APIBuild() // → "3.14.0-gcc-11.2.0-linux-x86_64"
func (h HandleType) APIBuild() string {
	build := C.qdb_build()
	defer h.Release(unsafe.Pointer(build))
	return C.GoString(build)
}

// Open initializes a handle with the specified protocol.
//
// Args:
//
//	protocol: Network protocol to use (e.g. ProtocolTCP)
//
// Returns:
//
//	error: Initialization error if any
//
// Note: No connection will be established. Not needed if you created your handle with NewHandle.
//
// Example:
//
//	err := h.Open(qdb.ProtocolTCP)
//	if err != nil {
//	    return err
//	}
func (h HandleType) Open(protocol Protocol) error {
	err := C.qdb_open(&h.handle, C.qdb_protocol_t(protocol))
	return wrapError(err, "handle_open", "protocol", protocol)
}

// SetTimeout sets the timeout for all network operations.
//
// Args:
//
//	timeout: Duration for network operations timeout
//
// Returns:
//
//	error: Configuration error if any
//
// Note: Lower timeouts increase risk of timeout errors. Server-side timeout might be shorter.
//
// Example:
//
//	err := h.SetTimeout(30 * time.Second)
//	if err != nil {
//	    return err
//	}
func (h HandleType) SetTimeout(timeout time.Duration) error {
	err := C.qdb_option_set_timeout(h.handle, C.int(timeout/time.Millisecond))
	return wrapError(err, "handle_set_timeout", "timeout_ms", timeout/time.Millisecond)
}

// Encryption is an encryption option.
type Encryption C.qdb_encryption_t

// Encryption values:
//
//	EncryptNone : No encryption.
//	EncryptAES : Uses aes gcm 256 encryption.
const (
	EncryptNone Encryption = C.qdb_crypt_none
	EncryptAES  Encryption = C.qdb_crypt_aes_gcm_256
)

// SetEncryption sets the encryption method for the handle.
//
// Args:
//
//	encryption: Encryption type (EncryptNone or EncryptAES)
//
// Returns:
//
//	error: Configuration error if any
//
// Note: Must be called before Connect. See AddClusterPublicKey for adding public key.
//
// Example:
//
//	err := h.SetEncryption(qdb.EncryptAES)
//	if err != nil {
//	    return err
//	}
func (h HandleType) SetEncryption(encryption Encryption) error {
	err := C.qdb_option_set_encryption(h.handle, C.qdb_encryption_t(encryption))
	return wrapError(err, "set_encryption", "type", encryption)
}

// jSONCredentialConfig holds username and secret key from JSON credential files.
type jSONCredentialConfig struct {
	Username  string `json:"username"`
	SecretKey string `json:"secret_key"`
}

// UserCredentialFromFile retrieves user credentials from a JSON file.
//
// Args:
//
//	userCredentialFile: Path to JSON file containing username and secret_key
//
// Returns:
//
//	string: Username from the file
//	string: Secret key from the file
//	error: File read or JSON parsing error if any
//
// Example:
//
//	user, secret, err := qdb.UserCredentialFromFile("/path/to/user.json")
//	if err != nil {
//	    return err
//	}
func UserCredentialFromFile(userCredentialFile string) (string, string, error) {
	fileConfig, err := os.ReadFile(userCredentialFile)
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

// ClusterKeyFromFile retrieves cluster public key from a file.
//
// Args:
//
//	clusterPublicKeyFile: Path to file containing cluster public key in PEM format
//
// Returns:
//
//	string: Cluster public key content
//	error: File read error if any
//
// Example:
//
//	key, err := qdb.ClusterKeyFromFile("/path/to/cluster.key")
//	if err != nil {
//	    return err
//	}
func ClusterKeyFromFile(clusterPublicKeyFile string) (string, error) {
	clusterPublicKey, err := os.ReadFile(clusterPublicKeyFile)
	if err != nil {
		return "", err
	}
	return string(clusterPublicKey), nil
}

// AddUserCredentials adds a username and secret key for authentication.
//
// Args:
//
//	name: Username for authentication
//	secret: User secret key/private key
//
// Returns:
//
//	error: Configuration error if any
//
// Example:
//
//	err := handle.AddUserCredentials("myuser", "mysecretkey")
//	if err != nil {
//	    return err
//	}
func (h HandleType) AddUserCredentials(name, secret string) error {
	username := convertToCharStar(name)
	defer releaseCharStar(username)
	userSecret := convertToCharStar(secret)
	defer releaseCharStar(userSecret)
	qdbErr := C.qdb_option_set_user_credentials(h.handle, username, userSecret)
	return wrapError(qdbErr, "add_user_credentials")
}

// AddClusterPublicKey adds the cluster public key for secure communication.
//
// Args:
//
//	secret: Cluster public key in PEM format
//
// Returns:
//
//	error: Configuration error if any
//
// Note: Must be called before Connect.
//
// Example:
//
//	err := handle.AddClusterPublicKey(clusterKey)
//	if err != nil {
//	    return err
//	}
func (h HandleType) AddClusterPublicKey(secret string) error {
	clusterPublicKey := convertToCharStar(secret)
	defer releaseCharStar(clusterPublicKey)
	qdbErr := C.qdb_option_set_cluster_public_key(h.handle, clusterPublicKey)
	return wrapError(qdbErr, "add_cluster_public_key")
}

// SetMaxCardinality sets the maximum allowed cardinality for queries.
//
// Args:
//
//	maxCardinality: Maximum cardinality value (minimum: 100)
//
// Returns:
//
//	error: Configuration error if any
//
// Note: Default value is 10,007. Minimum allowed value is 100.
//
// Example:
//
//	err := h.SetMaxCardinality(50000)
//	if err != nil {
//	    return err
//	}
func (h HandleType) SetMaxCardinality(maxCardinality uint) error {
	err := C.qdb_option_set_max_cardinality(h.handle, C.qdb_uint_t(maxCardinality))
	return wrapError(err, "handle_set_max_cardinality", "max_cardinality", maxCardinality)
}

// SetCompression sets the compression level for outgoing messages.
//
// Args:
//
//	compressionLevel: Compression type (CompNone, CompBalanced)
//
// Returns:
//
//	error: Configuration error if any
//
// Note: API can read any compression used by server regardless of this setting.
//
// Example:
//
//	err := h.SetCompression(qdb.CompBalanced)
//	if err != nil {
//	    return err
//	}
func (h HandleType) SetCompression(compressionLevel Compression) error {
	err := C.qdb_option_set_compression(h.handle, C.qdb_compression_t(compressionLevel))
	return makeErrorOrNil(err)
}

// SetClientMaxInBufSize sets the maximum incoming buffer size for client network operations.
//
// Args:
//
//	bufSize: Maximum buffer size in bytes
//
// Returns:
//
//	error: Configuration error if any
//
// Note: Only modify if expecting very large responses from server.
//
// Example:
//
//	err := h.SetClientMaxInBufSize(64 * 1024 * 1024) // 64MB
//	if err != nil {
//	    return err
//	}
func (h HandleType) SetClientMaxInBufSize(bufSize uint) error {
	err := C.qdb_option_set_client_max_in_buf_size(h.handle, C.size_t(bufSize))
	return makeErrorOrNil(err)
}

// GetClientMaxInBufSize gets the maximum incoming buffer size for client network operations.
//
// Args:
//
//	None
//
// Returns:
//
//	uint: Current maximum buffer size in bytes
//	error: Retrieval error if any
//
// Example:
//
//	size, err := h.GetClientMaxInBufSize() // → 16777216
//	if err != nil {
//	    return 0, err
//	}
func (h HandleType) GetClientMaxInBufSize() (uint, error) {
	var bufSize C.size_t
	err := C.qdb_option_get_client_max_in_buf_size(h.handle, &bufSize)
	return uint(bufSize), wrapError(err, "get_client_max_in_buf_size")
}

// GetClusterMaxInBufSize gets the maximum incoming buffer size allowed by the cluster.
//
// Args:
//
//	None
//
// Returns:
//
//	uint: Maximum buffer size allowed by cluster in bytes
//	error: Retrieval error if any
//
// Example:
//
//	size, err := h.GetClusterMaxInBufSize() // → 67108864
//	if err != nil {
//	    return 0, err
//	}
func (h HandleType) GetClusterMaxInBufSize() (uint, error) {
	var bufSize C.size_t
	err := C.qdb_option_get_cluster_max_in_buf_size(h.handle, &bufSize)
	return uint(bufSize), wrapError(err, "get_cluster_max_in_buf_size")
}

// GetClientMaxParallelism gets the maximum parallelism option of the client.
//
// Args:
//
//	None
//
// Returns:
//
//	uint: Current maximum parallelism thread count
//	error: Retrieval error if any
//
// Example:
//
//	count, err := h.GetClientMaxParallelism() // → 16
//	if err != nil {
//	    return 0, err
//	}
func (h HandleType) GetClientMaxParallelism() (uint, error) {
	var threadCount C.size_t
	err := C.qdb_option_get_client_max_parallelism(h.handle, &threadCount)
	return uint(threadCount), makeErrorOrNil(err)
}

// SetClientMaxParallelism sets the maximum parallelism level for the client.
//
// Args:
//
//	threadCount: Number of threads for concurrent operations
//
// Returns:
//
//	error: Configuration error if any
//
// Note: Higher values may improve throughput but consume more resources.
//
// Example:
//
//	err := h.SetClientMaxParallelism(16)
//	if err != nil {
//	    return err
//	}
func (h HandleType) SetClientMaxParallelism(threadCount uint) error {
	err := C.qdb_option_set_client_max_parallelism(h.handle, C.size_t(threadCount))
	return makeErrorOrNil(err)
}

// Connect connects a previously opened handle to a QuasarDB cluster.
//
// Args:
//
//	clusterURI: URI in format qdb://<address>:<port> (IPv4/IPv6/domain)
//
// Returns:
//
//	error: Connection error if any
//
// Example:
//
//	err := h.Connect("qdb://localhost:2836")
//	if err != nil {
//	    return err
//	}
//	// Multiple nodes: "qdb://node1:2836,node2:2836"
//	// IPv6: "qdb://[::1]:2836"
func (h HandleType) Connect(clusterURI string) error {
	uri := convertToCharStar(clusterURI)
	defer releaseCharStar(uri)
	err := C.qdb_connect(h.handle, uri)
	if err == C.qdb_e_ok {
		L().Info("successfully connected", "cluster", clusterURI)
	}
	return wrapError(err, "connect", "uri", clusterURI)
}

// Close closes the handle and releases all resources.
//
// Args:
//
//	None
//
// Returns:
//
//	error: Closing error if any
//
// Note: Terminates connections and releases all internal buffers.
//
// Example:
//
//	err := h.Close()
//	if err != nil {
//	    return err
//	}
func (h HandleType) Close() error {
	err := C.qdb_close(h.handle)
	return makeErrorOrNil(err)
}

// Release releases an API-allocated buffer.
//
// Args:
//
//	buffer: Pointer to buffer allocated by QuasarDB API
//
// Returns:
//
//	None
//
// Note: Failure to call may cause memory leaks. Works with any API-allocated buffer type.
//
// Example:
//
//	var tags **C.char
//	err := C.qdb_get_tags(h.handle, alias, &tags, &tagCount)
//	defer h.Release(unsafe.Pointer(tags))
func (h HandleType) Release(buffer unsafe.Pointer) {
	C.qdb_release(h.handle, buffer)
}

// GetTags retrieves all tags of an entry.
//
// Args:
//
//	entryAlias: Name of the entry to get tags from
//
// Returns:
//
//	[]string: List of tag names
//	error: Retrieval error if any
//
// Note: Entry must exist. Tags scale across nodes.
//
// Example:
//
//	tags, err := h.GetTags("myentry") // → ["important", "data"]
//	if err != nil {
//	    return nil, err
//	}
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

// GetTagged retrieves all entries that have the specified tag.
//
// Args:
//
//	tag: Tag name to search for
//
// Returns:
//
//	[]string: List of entry aliases with this tag
//	error: Retrieval error if any
//
// Note: Tag must exist. Constant time complexity.
//
// Example:
//
//	entries, err := h.GetTagged("important") // → ["entry1", "entry2"]
//	if err != nil {
//	    return nil, err
//	}
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
	return nil, wrapError(err, "get_tagged", "tag", tag)
}

// PrefixGet retrieves all entries matching the provided prefix.
//
// Args:
//
//	prefix: Prefix string to match
//	limit: Maximum number of results to return
//
// Returns:
//
//	[]string: List of entry aliases matching the prefix
//	error: Retrieval error if any
//
// Example:
//
//	entries, err := h.PrefixGet("user:", 100) // → ["user:1", "user:2"]
//	if err != nil {
//	    return nil, err
//	}
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

// PrefixCount retrieves the count of entries matching the provided prefix.
//
// Args:
//
//	prefix: Prefix string to match
//
// Returns:
//
//	uint64: Number of entries matching the prefix
//	error: Counting error if any
//
// Example:
//
//	count, err := h.PrefixCount("user:") // → 42
//	if err != nil {
//	    return 0, err
//	}
func (h HandleType) PrefixCount(prefix string) (uint64, error) {
	cPrefix := convertToCharStar(prefix)
	defer releaseCharStar(cPrefix)
	var count C.qdb_uint_t
	err := C.qdb_prefix_count(h.handle, cPrefix, &count)

	return uint64(count), makeErrorOrNil(err)
}

// Handles Creators

// NewHandle creates a new handle.
//
// Args:
//
//	None
//
// Returns:
//
//	HandleType: Opened handle (not connected) with TCP protocol
//	error: Creation error if any
//
// Example:
//
//	h, err := qdb.NewHandle()
//	if err != nil {
//	    return err
//	}
//	defer h.Close()
func NewHandle() (HandleType, error) {
	var h HandleType
	err := C.qdb_open((*C.qdb_handle_t)(&h.handle), C.qdb_protocol_t(ProtocolTCP))

	return h, makeErrorOrNil(err)
}

// NewHandleWithNativeLogs creates a new handle with native C++ logging enabled.
//
// Args:
//
//	None
//
// Returns:
//
//	HandleType: Opened handle with native logging enabled
//	error: Creation error if any
//
// Example:
//
//	h, err := qdb.NewHandleWithNativeLogs()
//	if err != nil {
//	    return err
//	}
//	defer h.Close()
func NewHandleWithNativeLogs() (HandleType, error) {
	h, err := NewHandle()
	if err != nil {
		return h, err
	}

	swapCallback()
	return h, nil
}

// NewHandleFromOptions creates and configures a new handle using the provided options.
//
// Args:
//
//	options: Configuration options for the handle
//
// Returns:
//
//	HandleType: Configured and connected handle
//	error: Creation or configuration error if any
//
// Example:
//
//	opts := qdb.NewHandleOptions().
//	    WithClusterUri("qdb://localhost:2836").
//	    WithTimeout(30 * time.Second)
//	h, err := qdb.NewHandleFromOptions(opts)
//	if err != nil {
//	    return err
//	}
//	defer h.Close()
func NewHandleFromOptions(options *HandleOptions) (HandleType, error) {
	// Validate options
	if err := options.validate(); err != nil {
		return HandleType{}, fmt.Errorf("invalid options: %w", err)
	}

	// Create new handle
	h, err := NewHandle()
	if err != nil {
		return HandleType{}, fmt.Errorf("failed to create handle: %w", err)
	}

	// Setup cleanup on error
	var setupErr error
	defer func() {
		if setupErr != nil {
			// Log close error but don't override the original error
			if closeErr := h.Close(); closeErr != nil {
				L().Debug("failed to close handle during cleanup", "error", closeErr)
			}
		}
	}()

	// Set compression (must be before Connect)
	if setupErr = h.SetCompression(options.compression); setupErr != nil {
		return HandleType{}, fmt.Errorf("failed to set compression: %w", setupErr)
	}

	// Set encryption (must be before Connect)
	if setupErr = h.SetEncryption(options.encryption); setupErr != nil {
		return HandleType{}, fmt.Errorf("failed to set encryption: %w", setupErr)
	}

	// Set timeout
	if setupErr = h.SetTimeout(options.timeout); setupErr != nil {
		return HandleType{}, fmt.Errorf("failed to set timeout: %w", setupErr)
	}

	// Set client max parallelism if specified
	if options.clientMaxParallelism > 0 {
		// Ensure the value fits in uint (validation already checks this)
		if setupErr = h.SetClientMaxParallelism(uint(options.clientMaxParallelism)); setupErr != nil {
			return HandleType{}, fmt.Errorf("failed to set client max parallelism: %w", setupErr)
		}
	}

	// Set client max in buffer size if specified
	if options.clientMaxInBufSize > 0 {
		if setupErr = h.SetClientMaxInBufSize(options.clientMaxInBufSize); setupErr != nil {
			return HandleType{}, fmt.Errorf("failed to set client max in buffer size: %w", setupErr)
		}
	}

	// Handle cluster public key
	if options.clusterPublicKeyFile != "" {
		clusterKey, err := ClusterKeyFromFile(options.clusterPublicKeyFile)
		if err != nil {
			setupErr = err
			return HandleType{}, fmt.Errorf("failed to load cluster public key from file: %w", err)
		}
		if setupErr = h.AddClusterPublicKey(clusterKey); setupErr != nil {
			return HandleType{}, fmt.Errorf("failed to add cluster public key: %w", setupErr)
		}
	} else if options.clusterPublicKey != "" {
		if setupErr = h.AddClusterPublicKey(options.clusterPublicKey); setupErr != nil {
			return HandleType{}, fmt.Errorf("failed to add cluster public key: %w", setupErr)
		}
	}

	// Handle user credentials
	if options.userSecurityFile != "" {
		userName, userSecret, err := UserCredentialFromFile(options.userSecurityFile)
		if err != nil {
			setupErr = err
			return HandleType{}, fmt.Errorf("failed to load user credentials from file: %w", err)
		}
		if setupErr = h.AddUserCredentials(userName, userSecret); setupErr != nil {
			return HandleType{}, fmt.Errorf("failed to add user credentials: %w", setupErr)
		}
	} else if options.userName != "" && options.userSecret != "" {
		if setupErr = h.AddUserCredentials(options.userName, options.userSecret); setupErr != nil {
			return HandleType{}, fmt.Errorf("failed to add user credentials: %w", setupErr)
		}
	}

	// Connect to cluster
	if setupErr = h.Connect(options.clusterURI); setupErr != nil {
		return HandleType{}, fmt.Errorf("failed to connect to cluster: %w", setupErr)
	}

	// Success - clear setupErr to prevent cleanup
	setupErr = nil
	return h, nil
}

// SetupHandle creates and connects a handle to a QuasarDB cluster.
//
// Args:
//
//	clusterURI: URI of the QuasarDB cluster (e.g. "qdb://localhost:2836")
//	timeout: Network operation timeout
//
// Returns:
//
//	HandleType: Connected handle
//	error: Creation or connection error if any
//
// Example:
//
//	h, err := qdb.SetupHandle("qdb://localhost:2836", 30*time.Second)
//	if err != nil {
//	    return err
//	}
//	defer h.Close()
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

// MustSetupHandle creates and connects a handle, panics on error.
//
// Args:
//
//	clusterURI: URI of the QuasarDB cluster (e.g. "qdb://localhost:2836")
//	timeout: Network operation timeout
//
// Returns:
//
//	HandleType: Connected handle
//
// Example:
//
//	h := qdb.MustSetupHandle("qdb://localhost:2836", 30*time.Second)
//	defer h.Close()
func MustSetupHandle(clusterURI string, timeout time.Duration) HandleType {
	h, err := SetupHandle(clusterURI, timeout)
	if err != nil {
		panic(err)
	}
	return h
}

// SetupSecuredHandle creates and connects a secured handle with authentication.
//
// Args:
//
//	clusterURI: URI of the QuasarDB cluster
//	clusterPublicKeyFile: Path to cluster public key file
//	userCredentialFile: Path to user credentials JSON file
//	timeout: Network operation timeout
//	encryption: Encryption type to use
//
// Returns:
//
//	HandleType: Secured and connected handle
//	error: Creation, security setup, or connection error if any
//
// Example:
//
//	h, err := qdb.SetupSecuredHandle(
//	    "qdb://secure-cluster:2838",
//	    "/path/to/cluster.key",
//	    "/path/to/user.json",
//	    30*time.Second,
//	    qdb.EncryptAES)
//	if err != nil {
//	    return err
//	}
//	defer h.Close()
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

// MustSetupSecuredHandle creates and connects a secured handle, panics on error.
//
// Args:
//
//	clusterURI: URI of the QuasarDB cluster
//	clusterPublicKeyFile: Path to cluster public key file
//	userCredentialFile: Path to user credentials JSON file
//	timeout: Network operation timeout
//	encryption: Encryption type to use
//
// Returns:
//
//	HandleType: Secured and connected handle
//
// Example:
//
//	h := qdb.MustSetupSecuredHandle(
//	    "qdb://secure-cluster:2838",
//	    "/path/to/cluster.key",
//	    "/path/to/user.json",
//	    30*time.Second,
//	    qdb.EncryptAES)
//	defer h.Close()
func MustSetupSecuredHandle(clusterURI, clusterPublicKeyFile, userCredentialFile string, timeout time.Duration, encryption Encryption) HandleType {
	h, err := SetupSecuredHandle(clusterURI, clusterPublicKeyFile, userCredentialFile, timeout, encryption)
	if err != nil {
		panic(err)
	}
	return h
}

// Entries creators

// Blob creates an entry accessor for blob operations.
//
// Args:
//
//	alias: Name of the blob entry
//
// Returns:
//
//	BlobEntry: Blob entry accessor
//
// Example:
//
//	blob := h.Blob("my_data")
//	err := blob.Put([]byte("Hello World"))
func (h HandleType) Blob(alias string) BlobEntry {
	return BlobEntry{Entry{h, alias}}
}

// Integer creates an entry accessor for integer operations.
//
// Args:
//
//	alias: Name of the integer entry
//
// Returns:
//
//	IntegerEntry: Integer entry accessor
//
// Example:
//
//	counter := h.Integer("my_counter")
//	err := counter.Put(42)
func (h HandleType) Integer(alias string) IntegerEntry {
	return IntegerEntry{Entry{h, alias}}
}

// Timeseries creates an entry accessor for timeseries operations.
//
// Args:
//
//	alias: Name of the timeseries table
//
// Returns:
//
//	TimeseriesEntry: Timeseries table accessor
//
// Deprecated: Use Table instead.
//
// Example:
//
//	ts := h.Timeseries("measurements")
func (h HandleType) Timeseries(alias string) TimeseriesEntry {
	return h.Table(alias)
}

// Table creates an entry accessor for timeseries table operations.
//
// Args:
//
//	alias: Name of the timeseries table
//
// Returns:
//
//	TimeseriesEntry: Timeseries table accessor
//
// Example:
//
//	table := h.Table("measurements")
//	err := table.Create(columns...)
func (h HandleType) Table(alias string) TimeseriesEntry {
	return TimeseriesEntry{Entry{h, alias}}
}

// Node creates an entry accessor for node operations.
//
// Args:
//
//	uri: URI of the node (e.g. "qdb://localhost:2836")
//
// Returns:
//
//	*Node: Node accessor
//
// Example:
//
//	node := h.Node("qdb://localhost:2836")
//	status, err := node.Status()
func (h HandleType) Node(uri string) *Node {
	return &Node{h, uri}
}

// Find creates an entry accessor for find query operations.
//
// Args:
//
//	None
//
// Returns:
//
//	*Find: Find query builder
//
// Example:
//
//	results, err := h.Find().
//	    Tag("important").
//	    Type(qdb.EntryBlob).
//	    Execute()
func (h HandleType) Find() *Find {
	return &Find{h, []string{}, []string{}, []string{}}
}

// Cluster creates an entry accessor for cluster operations.
//
// Args:
//
//	None
//
// Returns:
//
//	*Cluster: Cluster operations accessor
//
// Example:
//
//	cluster := h.Cluster()
//	err := cluster.TrimAll()
func (h HandleType) Cluster() *Cluster {
	return &Cluster{h}
}

// Query creates an entry accessor for query operations.
//
// Args:
//
//	query: SQL-like query string
//
// Returns:
//
//	*Query: Query executor
//
// Example:
//
//	q := h.Query("SELECT * FROM measurements WHERE value > 100")
//	result, err := q.Execute()
func (h HandleType) Query(query string) *Query {
	return &Query{h, query}
}

// TsBatch creates an entry accessor for batch timeseries operations.
//
// Args:
//
//	cols: Column information for batch operations
//
// Returns:
//
//	*TsBatch: Batch operations accessor
//	error: Creation error if any
//
// Example:
//
//	batch, err := h.TsBatch(columns...)
//	if err != nil {
//	    return nil, err
//	}
//	defer batch.Release()
func (h HandleType) TsBatch(cols ...TsBatchColumnInfo) (*TsBatch, error) {
	columns := tsBatchColumnInfoArrayToC(cols...)
	defer releaseTsBatchColumnInfoArray(columns, len(cols))
	columnsCount := C.qdb_size_t(len(cols))
	batch := &TsBatch{}
	batch.h = h
	err := C.qdb_ts_batch_table_init(h.handle, columns, columnsCount, &batch.table)
	return batch, wrapError(err, "ts_batch_init", "columns", len(cols))
}

// GetLastError retrieves last operation error.
//
// Args:
//
//	None
//
// Returns:
//
//	string: Error message text
//	error: Error code
//
// Example:
//
//	msg, err := h.GetLastError() // → "Connection timeout"
func (h HandleType) GetLastError() (string, error) {
	var err C.qdb_error_t
	var message *C.qdb_string_t = nil
	C.qdb_get_last_error(h.handle, &err, &message)
	defer h.Release(unsafe.Pointer(message))
	lastError := ""
	if message != nil {
		lastError = C.GoString(message.data)
	}
	return lastError, makeErrorOrNil(err)
}
