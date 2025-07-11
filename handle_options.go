// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// HandleOptions holds all configuration options for creating a handle.
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
//	    WithClientMaxInBufSize(64 * 1024 * 1024)
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

// WithClusterPublicKeyFile sets the cluster public key file path option.
func (o *HandleOptions) WithClusterPublicKeyFile(path string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clusterPublicKeyFile = path

	return &opts
}

// WithClusterPublicKey sets the cluster public key option.
func (o *HandleOptions) WithClusterPublicKey(key string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.clusterPublicKey = key

	return &opts
}

// WithUserSecurityFile sets the user security file path option.
func (o *HandleOptions) WithUserSecurityFile(path string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userSecurityFile = path

	return &opts
}

// WithUserName sets the username option.
func (o *HandleOptions) WithUserName(name string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userName = name

	return &opts
}

// WithUserSecret sets the user secret option.
func (o *HandleOptions) WithUserSecret(secret string) *HandleOptions {
	// Create a copy to maintain immutability
	opts := *o
	opts.userSecret = secret

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

// GetTimeout returns the current timeout value.
func (o *HandleOptions) GetTimeout() time.Duration {
	return o.timeout
}

// validate checks options consistency
// In: HandleOptions to validate
// Out: error if invalid
// Ex: validate() → nil|error
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
		encryption:           provider.GetEncryption(),
		compression:          provider.GetCompression(),
		clientMaxParallelism: provider.GetClientMaxParallelism(),
		clientMaxInBufSize:   provider.GetClientMaxInBufSize(),
		timeout:              provider.GetTimeout(),
	}
}
