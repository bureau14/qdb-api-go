package qdb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// useOneSession dials, does one round trip and closes: what a pool does
// with a session over its life.
func useOneSession(f *SessionFactory) error {
	s, err := f.NewSession()
	if err != nil {
		return err
	}
	blob := s.Blob(generateAlias(16))
	err = blob.Put([]byte("session"), NeverExpires())
	if err != nil {
		return err
	}
	err = blob.Remove()
	if err != nil {
		return err
	}

	return s.Close()
}

// One factory, many concurrent sessions, each closed on its own; the
// factory dials again afterwards. Pins that sessions are independent of
// each other and of the factory.
func TestSessionFactoryNewSessionIndependent(t *testing.T) {
	f := NewSessionFactory(NewHandleOptions().WithClusterUri(insecureURI))

	const n = 8
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- useOneSession(f)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.NoError(t, useOneSession(f))
}

// The two authentication paths against the secure cluster: both halves
// from files (read by the C API) and a key file with inline credentials
// (the file half read in Go).
func TestSessionFactorySecure(t *testing.T) {
	user, secret, err := UserCredentialFromFile(userPrivateKeyFile)
	require.NoError(t, err)

	base := NewHandleOptions().WithClusterUri(secureURI).WithClusterPublicKeyFile(clusterPublicKeyFile)
	tests := []struct {
		name string
		opts *HandleOptions
	}{
		{"both files", base.WithUserSecurityFile(userPrivateKeyFile)},
		{"key file with inline user", base.WithUserName(user).WithUserSecret(secret)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, useOneSession(NewSessionFactory(tt.opts)))
		})
	}
}
