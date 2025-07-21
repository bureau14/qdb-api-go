package qdb

import (
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

func TestHandleOptionsValidation(t *testing.T) {
	tests := []struct {
		name    string
		opts    *HandleOptions
		wantErr string
	}{
		{
			name:    "missing cluster URI",
			opts:    &HandleOptions{},
			wantErr: "cluster URI is required",
		},
		{
			name: "invalid cluster URI",
			opts: &HandleOptions{
				clusterURI: "http://localhost:2836",
			},
			wantErr: "cluster URI must start with 'qdb://'",
		},
		{
			name: "empty cluster URI after trim",
			opts: &HandleOptions{
				clusterURI: "   ",
			},
			wantErr: "cluster URI is required",
		},
		{
			name: "whitespace-only user name",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				userName:   "   ",
				userSecret: "valid-secret",
			},
			wantErr: "both user name and user secret must be provided together",
		},
		{
			name: "whitespace-only user secret",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				userName:   "valid-user",
				userSecret: "   ",
			},
			wantErr: "both user name and user secret must be provided together",
		},
		{
			name: "both cluster key methods",
			opts: &HandleOptions{
				clusterURI:           "qdb://localhost:2836",
				clusterPublicKeyFile: "key.file",
				clusterPublicKey:     "direct-key",
			},
			wantErr: "cannot specify both cluster public key file and direct cluster public key",
		},
		{
			name: "both user credential methods",
			opts: &HandleOptions{
				clusterURI:       "qdb://localhost:2836",
				userSecurityFile: "user.file",
				userName:         "user",
			},
			wantErr: "cannot specify both user security file and direct user credentials",
		},
		{
			name: "missing user secret",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				userName:   "user",
			},
			wantErr: "both user name and user secret must be provided together",
		},
		{
			name: "missing user name",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				userSecret: "secret",
			},
			wantErr: "both user name and user secret must be provided together",
		},
		{
			name: "cluster key without user credentials",
			opts: &HandleOptions{
				clusterURI:       "qdb://localhost:2836",
				clusterPublicKey: "key",
			},
			wantErr: "user credentials are required when cluster public key is set",
		},
		{
			name: "encryption without cluster key",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				encryption: EncryptAES,
			},
			wantErr: "encryption requires cluster public key to be set",
		},
		{
			name: "encryption without user credentials",
			opts: &HandleOptions{
				clusterURI:       "qdb://localhost:2836",
				encryption:       EncryptAES,
				clusterPublicKey: "key",
			},
			wantErr: "encryption requires user credentials to be set",
		},
		{
			name: "encryption with cluster key file but no user credentials",
			opts: &HandleOptions{
				clusterURI:           "qdb://localhost:2836",
				encryption:           EncryptAES,
				clusterPublicKeyFile: "cluster.key",
			},
			wantErr: "encryption requires user credentials to be set",
		},
		{
			name: "encryption with user credentials but no cluster key",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				encryption: EncryptAES,
				userName:   "user",
				userSecret: "secret",
			},
			wantErr: "encryption requires cluster public key to be set",
		},
		{
			name: "negative client max parallelism",
			opts: &HandleOptions{
				clusterURI:           "qdb://localhost:2836",
				clientMaxParallelism: -1,
			},
			wantErr: "client max parallelism cannot be negative",
		},
		{
			name: "client max parallelism overflow",
			opts: &HandleOptions{
				clusterURI:           "qdb://localhost:2836",
				clientMaxParallelism: 65537,
			},
			wantErr: "client max parallelism 65537 exceeds maximum allowed value 65536",
		},
		{
			name: "negative timeout",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				timeout:    -1 * time.Second,
			},
			wantErr: "timeout cannot be negative",
		},
		{
			name: "zero timeout",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				timeout:    0,
			},
			wantErr: "timeout cannot be zero",
		},
		{
			name: "timeout overflow",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				timeout:    25 * 24 * time.Hour, // > 24.8 days
			},
			wantErr: "timeout 600h0m0s exceeds maximum allowed value (approximately 24.8 days)",
		},
		{
			name: "valid unsecured",
			opts: &HandleOptions{
				clusterURI: "qdb://localhost:2836",
				timeout:    30 * time.Second,
			},
			wantErr: "",
		},
		{
			name: "valid secured with files",
			opts: &HandleOptions{
				clusterURI:           "qdb://localhost:2836",
				clusterPublicKeyFile: "cluster.key",
				userSecurityFile:     "user.json",
				timeout:              30 * time.Second,
			},
			wantErr: "",
		},
		{
			name: "valid secured with direct credentials",
			opts: &HandleOptions{
				clusterURI:       "qdb://localhost:2836",
				clusterPublicKey: "key",
				userName:         "user",
				userSecret:       "secret",
				timeout:          30 * time.Second,
			},
			wantErr: "",
		},
		{
			name: "valid encrypted with all requirements",
			opts: &HandleOptions{
				clusterURI:       "qdb://localhost:2836",
				encryption:       EncryptAES,
				clusterPublicKey: "key",
				userName:         "user",
				userSecret:       "secret",
				timeout:          30 * time.Second,
			},
			wantErr: "",
		},
		{
			name: "valid encrypted with files",
			opts: &HandleOptions{
				clusterURI:           "qdb://localhost:2836",
				encryption:           EncryptAES,
				clusterPublicKeyFile: "cluster.key",
				userSecurityFile:     "user.json",
				timeout:              30 * time.Second,
			},
			wantErr: "",
		},
		{
			name: "strings are trimmed during validation",
			opts: &HandleOptions{
				clusterURI:       "  qdb://localhost:2836  ",
				clusterPublicKey: "  key  ",
				userName:         "  user  ",
				userSecret:       "  secret  ",
				timeout:          30 * time.Second,
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to verify trimming behavior
			originalURI := tt.opts.clusterURI

			err := tt.opts.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("expected no error, got: %v", err)
				}

				// Verify strings were trimmed (for "strings are trimmed during validation" test case)
				if tt.name == "strings are trimmed during validation" {
					// All strings should have been trimmed
					if tt.opts.clusterURI != "qdb://localhost:2836" {
						t.Errorf("cluster URI not properly trimmed, got %q", tt.opts.clusterURI)
					}
					if tt.opts.clusterPublicKey != "key" {
						t.Errorf("cluster public key not properly trimmed, got %q", tt.opts.clusterPublicKey)
					}
					if tt.opts.userName != "user" {
						t.Errorf("user name not properly trimmed, got %q", tt.opts.userName)
					}
					if tt.opts.userSecret != "secret" {
						t.Errorf("user secret not properly trimmed, got %q", tt.opts.userSecret)
					}
				} else if tt.opts.clusterURI != "" && tt.opts.clusterURI != originalURI {
					if len(originalURI) > len(tt.opts.clusterURI) {
						// String was trimmed, which is expected
						t.Logf("cluster URI was trimmed from %q to %q", originalURI, tt.opts.clusterURI)
					}
				}
			} else {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.wantErr)
				} else if err.Error() != tt.wantErr {
					t.Errorf("expected error %q, got %q", tt.wantErr, err.Error())
				}
			}
		})
	}
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
		if copied.GetTimeout() != original.GetTimeout() {
			t.Errorf("timeout mismatch")
		}
	})
}
