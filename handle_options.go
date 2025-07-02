package qdb

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// HandleOptions holds all configuration options for creating a handle.
//
// Example usage:
//
//	// Simple unsecured connection
//	opts := NewHandleOptions().
//	    WithClusterUri("qdb://localhost:2836").
//	    WithTimeout(30 * time.Second)
//	handle, err := qdb.NewHandleFromOptions(opts)
//
//	// Secured connection with files
//	opts := NewHandleOptions().
//	    WithClusterUri("qdb://secure-cluster:2838").
//	    WithClusterPublicKeyFile("/path/to/cluster.key").
//	    WithUserSecurityFile("/path/to/user.json").
//	    WithEncryption(qdb.EncryptAES)
//	handle, err := qdb.NewHandleFromOptions(opts)
//
//	// High-performance configuration
//	opts := NewHandleOptions().
//	    WithClusterUri("qdb://cluster:2836").
//	    WithCompression(qdb.CompNone).
//	    WithClientMaxParallelism(16).
//	    WithClientMaxInBufSize(64 * 1024 * 1024)
//	handle, err := qdb.NewHandleFromOptions(opts)
type HandleOptions struct {
	// Connection
	clusterURI string

	// Security - Cluster
	clusterPublicKeyFile string
	clusterPublicKey     string

	// Security - User
	userSecurityFile string
	userName         string
	userSecret       string

	// Network & Performance
	encryption           Encryption
	compression          Compression
	clientMaxParallelism int
	clientMaxInBufSize   uint
	timeout              time.Duration
}

// NewHandleOptions creates a new HandleOptions with default values.
//
// Default values:
//   - Compression: CompBalanced (changed from CompFast in v3.x)
//   - Encryption: EncryptNone
//   - Timeout: 120 seconds
func NewHandleOptions() *HandleOptions {
	return &HandleOptions{
		compression: CompBalanced,
		encryption:  EncryptNone,
		timeout:     120 * time.Second,
	}
}

// WithClusterUri sets the cluster URI to connect to
func (o *HandleOptions) WithClusterUri(uri string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clusterURI = uri
	return &opts
}

// WithClusterPublicKeyFile sets the path to the cluster public key file
func (o *HandleOptions) WithClusterPublicKeyFile(path string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clusterPublicKeyFile = path
	return &opts
}

// WithClusterPublicKey sets the cluster public key directly
func (o *HandleOptions) WithClusterPublicKey(key string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clusterPublicKey = key
	return &opts
}

// WithUserSecurityFile sets the path to the user security file containing credentials
func (o *HandleOptions) WithUserSecurityFile(path string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userSecurityFile = path
	return &opts
}

// WithUserName sets the username for authentication
func (o *HandleOptions) WithUserName(name string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userName = name
	return &opts
}

// WithUserSecret sets the user secret/private key for authentication
func (o *HandleOptions) WithUserSecret(secret string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userSecret = secret
	return &opts
}

// WithEncryption sets the encryption type
func (o *HandleOptions) WithEncryption(encryption Encryption) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.encryption = encryption
	return &opts
}

// WithCompression sets the compression type
func (o *HandleOptions) WithCompression(compression Compression) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.compression = compression
	return &opts
}

// WithClientMaxParallelism sets the maximum parallelism for the client
func (o *HandleOptions) WithClientMaxParallelism(n int) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clientMaxParallelism = n
	return &opts
}

// WithClientMaxInBufSize sets the maximum incoming buffer size
func (o *HandleOptions) WithClientMaxInBufSize(size uint) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clientMaxInBufSize = size
	return &opts
}

// WithTimeout sets the network operation timeout
func (o *HandleOptions) WithTimeout(timeout time.Duration) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.timeout = timeout
	return &opts
}

// GetClusterURI returns the cluster URI
func (o *HandleOptions) GetClusterURI() string {
	return o.clusterURI
}

// GetClusterPublicKeyFile returns the cluster public key file path
func (o *HandleOptions) GetClusterPublicKeyFile() string {
	return o.clusterPublicKeyFile
}

// GetClusterPublicKey returns the cluster public key
func (o *HandleOptions) GetClusterPublicKey() string {
	return o.clusterPublicKey
}

// GetUserSecurityFile returns the user security file path
func (o *HandleOptions) GetUserSecurityFile() string {
	return o.userSecurityFile
}

// GetUserName returns the user name
func (o *HandleOptions) GetUserName() string {
	return o.userName
}

// GetUserSecret returns the user secret
// Note: This method is kept for internal use but should be used carefully for security reasons
func (o *HandleOptions) GetUserSecret() string {
	return o.userSecret
}

// GetEncryption returns the encryption type
func (o *HandleOptions) GetEncryption() Encryption {
	return o.encryption
}

// GetCompression returns the compression type
func (o *HandleOptions) GetCompression() Compression {
	return o.compression
}

// GetClientMaxParallelism returns the client max parallelism setting
func (o *HandleOptions) GetClientMaxParallelism() int {
	return o.clientMaxParallelism
}

// GetClientMaxInBufSize returns the client max input buffer size
func (o *HandleOptions) GetClientMaxInBufSize() uint {
	return o.clientMaxInBufSize
}

// GetTimeout returns the timeout duration
func (o *HandleOptions) GetTimeout() time.Duration {
	return o.timeout
}

// validate checks that the options are consistent and applies sanitization
func (o *HandleOptions) validate() error {
	// Trim all string fields first and apply them back
	o.clusterURI = strings.TrimSpace(o.clusterURI)
	o.clusterPublicKeyFile = strings.TrimSpace(o.clusterPublicKeyFile)
	o.clusterPublicKey = strings.TrimSpace(o.clusterPublicKey)
	o.userSecurityFile = strings.TrimSpace(o.userSecurityFile)
	o.userName = strings.TrimSpace(o.userName)
	o.userSecret = strings.TrimSpace(o.userSecret)

	// Check cluster URI is provided and valid
	if o.clusterURI == "" {
		return errors.New("cluster URI is required")
	}
	if !strings.HasPrefix(o.clusterURI, "qdb://") {
		return errors.New("cluster URI must start with 'qdb://'")
	}

	// Check that only one cluster public key method is used
	if o.clusterPublicKeyFile != "" && o.clusterPublicKey != "" {
		return errors.New("cannot specify both cluster public key file and direct cluster public key")
	}

	// Check that only one user credential method is used
	if o.userSecurityFile != "" && (o.userName != "" || o.userSecret != "") {
		return errors.New("cannot specify both user security file and direct user credentials")
	}

	// If direct user credentials are provided, both must be present
	if (o.userName != "" && o.userSecret == "") || (o.userName == "" && o.userSecret != "") {
		return errors.New("both user name and user secret must be provided together")
	}

	// Encryption validation: if encryption is enabled, require BOTH cluster public key AND user credentials
	if o.encryption != EncryptNone {
		hasClusterKey := o.clusterPublicKeyFile != "" || o.clusterPublicKey != ""
		hasUserCreds := o.userSecurityFile != "" || (o.userName != "" && o.userSecret != "")

		if !hasClusterKey {
			return errors.New("encryption requires cluster public key to be set")
		}
		if !hasUserCreds {
			return errors.New("encryption requires user credentials to be set")
		}
	}

	// If cluster public key is set, user credentials must also be set
	hasClusterKey := o.clusterPublicKeyFile != "" || o.clusterPublicKey != ""
	hasUserCreds := o.userSecurityFile != "" || (o.userName != "" && o.userSecret != "")

	if hasClusterKey && !hasUserCreds {
		return errors.New("user credentials are required when cluster public key is set")
	}

	// Validate client max parallelism
	if o.clientMaxParallelism < 0 {
		return errors.New("client max parallelism cannot be negative")
	}
	// Check if the value fits in uint32 (since C API expects qdb_size_t which maps to size_t/uint)
	if o.clientMaxParallelism > math.MaxUint32 {
		return fmt.Errorf("client max parallelism %d exceeds maximum allowed value %d", o.clientMaxParallelism, math.MaxUint32)
	}

	// Validate timeout
	if o.timeout < 0 {
		return errors.New("timeout cannot be negative")
	}
	if o.timeout == 0 {
		return errors.New("timeout cannot be zero")
	}
	// Check if timeout in milliseconds fits in int32 (C API expects int)
	timeoutMillis := o.timeout / time.Millisecond
	if timeoutMillis > math.MaxInt32 {
		return fmt.Errorf("timeout %v exceeds maximum allowed value (approximately 24.8 days)", o.timeout)
	}

	return nil
}

// HandleOptionsProvider provides methods to retrieve handle configuration values.
// Note: User secrets are not exposed through this interface for security reasons.
type HandleOptionsProvider interface {
	GetClusterURI() string
	GetClusterPublicKeyFile() string
	GetClusterPublicKey() string
	GetUserSecurityFile() string
	GetUserName() string
	// GetUserSecret() is intentionally omitted for security
	GetEncryption() Encryption
	GetCompression() Compression
	GetClientMaxParallelism() int
	GetClientMaxInBufSize() uint
	GetTimeout() time.Duration
}

// FromHandleOptionsProvider creates HandleOptions from a HandleOptionsProvider.
// Note: User secrets cannot be copied through this method for security reasons.
// If user credentials are needed, use WithUserSecurityFile or set them directly.
func FromHandleOptionsProvider(provider HandleOptionsProvider) *HandleOptions {
	if provider == nil {
		return nil
	}

	return &HandleOptions{
		clusterURI:           provider.GetClusterURI(),
		clusterPublicKeyFile: provider.GetClusterPublicKeyFile(),
		clusterPublicKey:     provider.GetClusterPublicKey(),
		userSecurityFile:     provider.GetUserSecurityFile(),
		userName:             provider.GetUserName(),
		// userSecret is intentionally not copied for security
		encryption:           provider.GetEncryption(),
		compression:          provider.GetCompression(),
		clientMaxParallelism: provider.GetClientMaxParallelism(),
		clientMaxInBufSize:   provider.GetClientMaxInBufSize(),
		timeout:              provider.GetTimeout(),
	}
}
