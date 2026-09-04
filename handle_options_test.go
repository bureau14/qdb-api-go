package qdb

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestHandleOptions(t *testing.T) {
	t.Run("NewHandleOptions with defaults", func(t *testing.T) {
		opts := NewHandleOptions()

		if opts.compression != CompBalanced {
			t.Errorf("expected default compression %v, got %v", CompBalanced, opts.compression)
		}
		if opts.encryption != EncryptNone {
			t.Errorf("expected default encryption %v, got %v", EncryptNone, opts.encryption)
		}
		if opts.timeout != 120*time.Second {
			t.Errorf("expected default timeout %v, got %v", 120*time.Second, opts.timeout)
		}
	})

	t.Run("WithClusterUri", func(t *testing.T) {
		uri := "qdb://localhost:2836"
		opts := NewHandleOptions().WithClusterUri(uri)

		if opts.clusterURI != uri {
			t.Errorf("expected cluster URI %q, got %q", uri, opts.clusterURI)
		}
	})

	t.Run("All options using builder pattern", func(t *testing.T) {
		opts := NewHandleOptions().
			WithClusterUri("qdb://localhost:2836").
			WithClusterPublicKey("test-key").
			WithUserName("test-user").
			WithUserSecret("test-secret").
			WithEncryption(EncryptAES).
			WithCompression(CompNone).
			WithClientMaxParallelism(8).
			WithClientMaxInBufSize(1024 * 1024).
			WithConnectionsPerAddress(16).
			WithTimeout(30 * time.Second)

		if opts.clusterURI != "qdb://localhost:2836" {
			t.Errorf("unexpected cluster URI: %q", opts.clusterURI)
		}
		if opts.clusterPublicKey != "test-key" {
			t.Errorf("unexpected cluster public key: %q", opts.clusterPublicKey)
		}
		if opts.userName != "test-user" {
			t.Errorf("unexpected user name: %q", opts.userName)
		}
		if opts.userSecret != "test-secret" {
			t.Errorf("unexpected user secret: %q", opts.userSecret)
		}
		if opts.encryption != EncryptAES {
			t.Errorf("unexpected encryption: %v", opts.encryption)
		}
		if opts.compression != CompNone {
			t.Errorf("unexpected compression: %v", opts.compression)
		}
		if opts.clientMaxParallelism != 8 {
			t.Errorf("unexpected client max parallelism: %d", opts.clientMaxParallelism)
		}
		if opts.clientMaxInBufSize != 1024*1024 {
			t.Errorf("unexpected client max in buf size: %d", opts.clientMaxInBufSize)
		}
		if opts.connectionsPerAddress != 16 {
			t.Errorf("unexpected connections per address: %d", opts.connectionsPerAddress)
		}
		if opts.timeout != 30*time.Second {
			t.Errorf("unexpected timeout: %v", opts.timeout)
		}
	})

	t.Run("Getter methods", func(t *testing.T) {
		opts := NewHandleOptions().
			WithClusterUri("qdb://localhost:2836").
			WithClusterPublicKeyFile("/path/to/cluster.key").
			WithUserSecurityFile("/path/to/user.json").
			WithEncryption(EncryptAES).
			WithCompression(CompBalanced).
			WithClientMaxParallelism(16).
			WithClientMaxInBufSize(2048 * 1024).
			WithConnectionsPerAddress(32).
			WithTimeout(60 * time.Second)

		if opts.GetClusterURI() != "qdb://localhost:2836" {
			t.Errorf("GetClusterURI() returned unexpected value")
		}
		if opts.GetClusterPublicKeyFile() != "/path/to/cluster.key" {
			t.Errorf("GetClusterPublicKeyFile() returned unexpected value")
		}
		if opts.GetUserSecurityFile() != "/path/to/user.json" {
			t.Errorf("GetUserSecurityFile() returned unexpected value")
		}
		if opts.GetEncryption() != EncryptAES {
			t.Errorf("GetEncryption() returned unexpected value")
		}
		if opts.GetCompression() != CompBalanced {
			t.Errorf("GetCompression() returned unexpected value")
		}
		if opts.GetClientMaxParallelism() != 16 {
			t.Errorf("GetClientMaxParallelism() returned unexpected value")
		}
		if opts.GetClientMaxInBufSize() != 2048*1024 {
			t.Errorf("GetClientMaxInBufSize() returned unexpected value")
		}
		if opts.GetConnectionsPerAddress() != 32 {
			t.Errorf("GetConnectionsPerAddress() returned unexpected value")
		}
		if opts.GetTimeout() != 60*time.Second {
			t.Errorf("GetTimeout() returned unexpected value")
		}
	})

	t.Run("Immutability", func(t *testing.T) {
		// Test that WithX methods return a new instance
		original := NewHandleOptions().WithClusterUri("qdb://localhost:2836")
		modified := original.WithTimeout(60 * time.Second)

		// Original should be unchanged
		if original.timeout != 120*time.Second {
			t.Errorf("original options were modified, expected timeout %v, got %v", 120*time.Second, original.timeout)
		}
		// Modified should have new value
		if modified.timeout != 60*time.Second {
			t.Errorf("modified options have wrong timeout, expected %v, got %v", 60*time.Second, modified.timeout)
		}
		// Both should have the same cluster URI
		if original.clusterURI != modified.clusterURI {
			t.Errorf("cluster URI not preserved in copy")
		}
	})
}

func TestHandleOptionsProvider(t *testing.T) {
	// Test that HandleOptions implements HandleOptionsProvider
	var _ HandleOptionsProvider = (*HandleOptions)(nil)

	t.Run("FromHandleOptionsProvider with nil", func(t *testing.T) {
		opts := FromHandleOptionsProvider(nil)
		if opts != nil {
			t.Errorf("expected nil, got %v", opts)
		}
	})

	t.Run("FromHandleOptionsProvider with valid provider", func(t *testing.T) {
		original := NewHandleOptions().
			WithClusterUri("qdb://localhost:2836").
			WithClusterPublicKey("test-key").
			WithUserName("test-user").
			WithUserSecret("test-secret").
			WithEncryption(EncryptAES).
			WithCompression(CompNone).
			WithClientMaxParallelism(8).
			WithClientMaxInBufSize(1024 * 1024).
			WithConnectionsPerAddress(16).
			WithTimeout(30 * time.Second)

		copied := FromHandleOptionsProvider(original)

		// Verify that user secret is not copied for security reasons
		if copied.userSecret != "" {
			t.Errorf("user secret should not be copied through FromHandleOptionsProvider")
		}

		if copied.GetClusterURI() != original.GetClusterURI() {
			t.Errorf("cluster URI mismatch")
		}
		if copied.GetClusterPublicKey() != original.GetClusterPublicKey() {
			t.Errorf("cluster public key mismatch")
		}
		if copied.GetUserName() != original.GetUserName() {
			t.Errorf("user name mismatch")
		}
		// Note: GetUserSecret() is intentionally not tested as it's not exposed through the interface
		if copied.GetEncryption() != original.GetEncryption() {
			t.Errorf("encryption mismatch")
		}
		if copied.GetCompression() != original.GetCompression() {
			t.Errorf("compression mismatch")
		}
		if copied.GetClientMaxParallelism() != original.GetClientMaxParallelism() {
			t.Errorf("client max parallelism mismatch")
		}
		if copied.GetClientMaxInBufSize() != original.GetClientMaxInBufSize() {
			t.Errorf("client max in buf size mismatch")
		}
		if copied.GetConnectionsPerAddress() != original.GetConnectionsPerAddress() {
			t.Errorf("connections per address mismatch")
		}
		if copied.GetTimeout() != original.GetTimeout() {
			t.Errorf("timeout mismatch")
		}
	})
}

func TestNewHandleFromOptionsConnectionsPerAddress(t *testing.T) {
	opts := NewHandleOptions().
		WithClusterUri(insecureURI).
		WithConnectionsPerAddress(16)

	h, err := NewHandleFromOptions(opts)
	if err != nil {
		t.Fatalf("NewHandleFromOptions() failed: %v", err)
	}
	t.Cleanup(func() {
		_ = h.Close()
	})

	v, err := h.GetConnectionsPerAddress()
	if err != nil {
		t.Fatalf("GetConnectionsPerAddress() failed: %v", err)
	}
	if v != 16 {
		t.Errorf("expected connections per address 16, got %d", v)
	}
}

func TestHandleOptionsLastSetWins(t *testing.T) {
	t.Run("cluster key slot", func(t *testing.T) {
		opts := NewHandleOptions().WithClusterPublicKey("inline").WithClusterPublicKeyFile("cluster.key")
		if opts.clusterPublicKey != "" || opts.clusterPublicKeyFile != "cluster.key" {
			t.Errorf("file did not replace inline key: %+v", opts)
		}
		opts = opts.WithClusterPublicKey("inline")
		if opts.clusterPublicKey != "inline" || opts.clusterPublicKeyFile != "" {
			t.Errorf("inline key did not replace file: %+v", opts)
		}
	})

	t.Run("user slot", func(t *testing.T) {
		opts := NewHandleOptions().WithUserName("user").WithUserSecret("secret").WithUserSecurityFile("user.json")
		if opts.userName != "" || opts.userSecret != "" || opts.userSecurityFile != "user.json" {
			t.Errorf("file did not replace inline credentials: %+v", opts)
		}
		opts = opts.WithUserName("user")
		if opts.userName != "user" || opts.userSecurityFile != "" {
			t.Errorf("inline name did not replace file: %+v", opts)
		}
	})
}

// The only Go-side rejections left are values the C types cannot carry;
// everything else is the C API's to judge.
func TestNewHandleFromOptionsRejectsUnrepresentable(t *testing.T) {
	base := NewHandleOptions().WithClusterUri(insecureURI)
	tests := []struct {
		name string
		opts *HandleOptions
	}{
		{"negative parallelism", base.WithClientMaxParallelism(-1)},
		{"negative connections per address", base.WithConnectionsPerAddress(-1)},
		{"timeout beyond C int milliseconds", base.WithTimeout(time.Duration(math.MaxInt32+1) * time.Millisecond)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewHandleFromOptions(tt.opts)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got %v", err)
			}
		})
	}
}
