// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

import (
	"time"
)

// HandleOptions holds all configuration options for creating a handle.
type HandleOptions struct {
	// Connection
	clusterURI string

	// Security - Cluster: one slot, inline or by file. The C API holds one
	// key per handle, so the last With... call wins.
	clusterPublicKeyFile string
	clusterPublicKey     string

	// Security - User: one slot, inline name/secret or a security file, same
	// rule.
	userSecurityFile string
	userName         string
	userSecret       string

	// Network & Performance
	encryption            Encryption
	compression           Compression
	clientMaxParallelism  int
	clientMaxInBufSize    uint
	connectionsPerAddress int
	timeout               time.Duration
}

// NewHandleOptions creates a new HandleOptions builder.
//
// Args:
//
//	None
//
// Returns:
//
//	*HandleOptions: Builder for configuring handle options
//
// Default values:
//   - Compression: CompBalanced
//   - Encryption: EncryptNone
//   - Timeout: 120 seconds
//   - Connections per address: C API default
//
// Example:
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
//	    WithClientMaxInBufSize(64 * 1024 * 1024).
//	    WithConnectionsPerAddress(16)
//	handle, err := qdb.NewHandleFromOptions(opts)
func NewHandleOptions() *HandleOptions {
	return &HandleOptions{
		compression: CompBalanced,
		encryption:  EncryptNone,
		timeout:     120 * time.Second,
	}
}

// WithClusterUri sets the cluster URI option.
func (o *HandleOptions) WithClusterUri(uri string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clusterURI = uri

	return &opts
}

// WithClusterPublicKeyFile sets the cluster public key file path option,
// replacing an inline key.
func (o *HandleOptions) WithClusterPublicKeyFile(path string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clusterPublicKeyFile = path
	opts.clusterPublicKey = ""

	return &opts
}

// WithClusterPublicKey sets the cluster public key option, replacing a key
// file.
func (o *HandleOptions) WithClusterPublicKey(key string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clusterPublicKey = key
	opts.clusterPublicKeyFile = ""

	return &opts
}

// WithUserSecurityFile sets the user security file path option, replacing
// inline credentials.
func (o *HandleOptions) WithUserSecurityFile(path string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userSecurityFile = path
	opts.userName = ""
	opts.userSecret = ""

	return &opts
}

// WithUserName sets the username option, replacing a user security file.
func (o *HandleOptions) WithUserName(name string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userName = name
	opts.userSecurityFile = ""

	return &opts
}

// WithUserSecret sets the user secret option, replacing a user security
// file.
func (o *HandleOptions) WithUserSecret(secret string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userSecret = secret
	opts.userSecurityFile = ""

	return &opts
}

// WithEncryption sets the encryption option.
func (o *HandleOptions) WithEncryption(encryption Encryption) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.encryption = encryption

	return &opts
}

// WithCompression sets the compression option.
func (o *HandleOptions) WithCompression(compression Compression) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.compression = compression

	return &opts
}

// WithClientMaxParallelism sets the client max parallelism option.
func (o *HandleOptions) WithClientMaxParallelism(n int) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clientMaxParallelism = n

	return &opts
}

// WithClientMaxInBufSize sets the client max input buffer size option.
func (o *HandleOptions) WithClientMaxInBufSize(size uint) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clientMaxInBufSize = size

	return &opts
}

// WithConnectionsPerAddress sets the soft limit on connections per IP address.
// The limit is split evenly between synchronous and asynchronous pools; valid
// values are in [2, 100000]. A value of 0 keeps the C API default.
// The option is applied before connecting, as required by the C API.
func (o *HandleOptions) WithConnectionsPerAddress(n int) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.connectionsPerAddress = n

	return &opts
}

// WithTimeout sets the timeout option.
func (o *HandleOptions) WithTimeout(timeout time.Duration) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.timeout = timeout

	return &opts
}

// GetClusterURI returns the current cluster URI value.
func (o *HandleOptions) GetClusterURI() string {
	return o.clusterURI
}

// GetClusterPublicKeyFile returns the current cluster public key file path value.
func (o *HandleOptions) GetClusterPublicKeyFile() string {
	return o.clusterPublicKeyFile
}

// GetClusterPublicKey returns the current cluster public key value.
func (o *HandleOptions) GetClusterPublicKey() string {
	return o.clusterPublicKey
}

// GetUserSecurityFile returns the current user security file path value.
func (o *HandleOptions) GetUserSecurityFile() string {
	return o.userSecurityFile
}

// GetUserName returns the current username value.
func (o *HandleOptions) GetUserName() string {
	return o.userName
}

// GetUserSecret returns the current user secret value.
// Note: This method is kept for internal use but should be used carefully for security reasons.
func (o *HandleOptions) GetUserSecret() string {
	return o.userSecret
}

// GetEncryption returns the current encryption value.
func (o *HandleOptions) GetEncryption() Encryption {
	return o.encryption
}

// GetCompression returns the current compression value.
func (o *HandleOptions) GetCompression() Compression {
	return o.compression
}

// GetClientMaxParallelism returns the current client max parallelism value.
func (o *HandleOptions) GetClientMaxParallelism() int {
	return o.clientMaxParallelism
}

// GetClientMaxInBufSize returns the current client max input buffer size value.
func (o *HandleOptions) GetClientMaxInBufSize() uint {
	return o.clientMaxInBufSize
}

// GetConnectionsPerAddress returns the current connections per address value.
func (o *HandleOptions) GetConnectionsPerAddress() int {
	return o.connectionsPerAddress
}

// GetTimeout returns the current timeout value.
func (o *HandleOptions) GetTimeout() time.Duration {
	return o.timeout
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
	GetConnectionsPerAddress() int
	GetTimeout() time.Duration
}

// FromHandleOptionsProvider creates HandleOptions from a provider.
//
// Args:
//
//	provider: HandleOptionsProvider interface implementation
//
// Returns:
//
//	*HandleOptions: New options instance, nil if provider is nil
//
// Note: User secrets cannot be copied for security reasons.
//
// Example:
//
//	newOpts := qdb.FromHandleOptionsProvider(existingOpts)
//	if newOpts != nil {
//	    newOpts.WithUserSecurityFile("/path/to/user.json")
//	}
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
		encryption:            provider.GetEncryption(),
		compression:           provider.GetCompression(),
		clientMaxParallelism:  provider.GetClientMaxParallelism(),
		clientMaxInBufSize:    provider.GetClientMaxInBufSize(),
		connectionsPerAddress: provider.GetConnectionsPerAddress(),
		timeout:               provider.GetTimeout(),
	}
}
