package qdb

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 120 * time.Second

//----------------------------------------------------
// basic Handle construction/open/setup
//----------------------------------------------------

func TestHandleConnectWithoutHandle(t *testing.T) {
	var h HandleType
	err := h.Connect(insecureURI)
	assert.Error(t, err)
}

func TestHandleOpenWithRandomProtocol(t *testing.T) {
	var h HandleType
	err := h.Open(2) // undefined protocol
	assert.Error(t, err)
}

func TestHandleOpenWithTCP(t *testing.T) {
	var h HandleType
	require.NoError(t, h.Open(ProtocolTCP))
	err := h.Close()
	if err != nil {
		t.Errorf("Failed to close handle: %v", err)
	}
}

func TestHandleSetup(t *testing.T) {
	h, err := SetupHandle(insecureURI, testTimeout)
	require.NoError(t, err)
	err = h.Close()
	if err != nil {
		t.Errorf("Failed to close handle: %v", err)
	}
}

func TestHandleSetupSecureUriWithNormalSetup(t *testing.T) {
	_, err := SetupHandle(secureURI, testTimeout)
	assert.Error(t, err)
}

func TestHandleSetupZeroTimeout(t *testing.T) {
	_, err := SetupHandle(insecureURI, 0)
	assert.Error(t, err)
}

func TestHandleMustSetup(t *testing.T) {
	h := MustSetupHandle(insecureURI, testTimeout)
	err := h.Close()
	if err != nil {
		t.Errorf("Failed to close handle: %v", err)
	}
}

func TestHandleMustSetupInvalidParameters(t *testing.T) {
	assert.Panics(t, func() { MustSetupHandle("", 0) })
}

//----------------------------------------------------
// secured handle helpers
//----------------------------------------------------

func TestHandleSetupSecure(t *testing.T) {
	h, err := SetupSecuredHandle(secureURI, clusterPublicKeyFile, userPrivateKeyFile, testTimeout, EncryptNone)
	require.NoError(t, err)
	err = h.Close()
	if err != nil {
		t.Errorf("Failed to close handle: %v", err)
	}
}

func TestHandleSetupSecureInsecureURI(t *testing.T) {
	_, err := SetupSecuredHandle(insecureURI, clusterPublicKeyFile, userPrivateKeyFile, testTimeout, EncryptNone)
	assert.Error(t, err)
}

func TestHandleSetupSecureMissingClusterKey(t *testing.T) {
	_, err := SetupSecuredHandle(secureURI, "", userPrivateKeyFile, testTimeout, EncryptNone)
	assert.Error(t, err)
}

func TestHandleSetupSecureMissingUserKey(t *testing.T) {
	_, err := SetupSecuredHandle(secureURI, clusterPublicKeyFile, "", testTimeout, EncryptNone)
	assert.Error(t, err)
}

func TestHandleSetupSecureZeroTimeout(t *testing.T) {
	_, err := SetupSecuredHandle(secureURI, clusterPublicKeyFile, userPrivateKeyFile, 0, EncryptNone)
	assert.Error(t, err)
}

func TestHandleSetupSecureRandomEncrypt(t *testing.T) {
	_, err := SetupSecuredHandle(secureURI, clusterPublicKeyFile, userPrivateKeyFile, testTimeout, 123)
	assert.Error(t, err)
}

func TestHandleMustSetupSecure(t *testing.T) {
	h := MustSetupSecuredHandle(secureURI, clusterPublicKeyFile, userPrivateKeyFile, testTimeout, EncryptNone)
	err := h.Close()
	if err != nil {
		t.Errorf("Failed to close handle: %v", err)
	}
}

func TestHandleMustSetupSecureInvalidParameters(t *testing.T) {
	assert.Panics(t, func() {
		MustSetupSecuredHandle(insecureURI, "", "", 0, 123)
	})
}

//----------------------------------------------------
// NewHandle-based checks – no connection
//----------------------------------------------------

func TestHandleGetLastErrorAfterFailedConnect(t *testing.T) {
	h, err := NewHandle()
	require.NoError(t, err)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	err = h.Connect("")
	msg, last := h.GetLastError()

	assert.True(t, errors.Is(err, last), "Connect error should contain GetLastError")
	assert.Equal(t, "at qdb_connect: Got NULL uri", msg)
}

func TestHandleClusterKeyFromFileInvalidName(t *testing.T) {
	_, err := ClusterKeyFromFile("nonexistent.file")
	assert.Error(t, err)
}

func TestHandleClusterKeyFromFileValid(t *testing.T) {
	f := createTempFile(t, "clusterkey", "PPm6ZeBCVlDTR9xtYasXd31s8rXnQpb+CNTMohOlQqBw=")
	_, err := ClusterKeyFromFile(f)
	assert.NoError(t, err)
}

func TestHandleCredentialsFromFileInvalidName(t *testing.T) {
	_, _, err := UserCredentialFromFile("nonexistent.file")
	assert.Error(t, err)
}

func TestHandleCredentialsFromFileInvalidContent(t *testing.T) {
	_, _, err := UserCredentialFromFile("error.go") // existing but not JSON
	assert.Error(t, err)
}

func TestHandleCredentialsFromFileValid(t *testing.T) {
	content := `{"username":"vianney","secret_key":"SeVUamemy6GWb8npfh9lum1zhdAu76W+l0PAW03G5yl4="}`
	f := createTempFile(t, "cred", content)

	user, secret, err := UserCredentialFromFile(f)
	require.NoError(t, err)
	assert.Equal(t, "vianney", user)
	assert.Equal(t, "SeVUamemy6GWb8npfh9lum1zhdAu76W+l0PAW03G5yl4=", secret)
}

//----------------------------------------------------
// Connected handle checks
//----------------------------------------------------

func TestHandleConnectWithoutAddress(t *testing.T) {
	h, err := NewHandle()
	require.NoError(t, err)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.Error(t, h.Connect(""))
}

func TestHandleConnectAndClose(t *testing.T) {
	h := newTestHandle(t)
	err := h.Close()
	if err != nil {
		t.Errorf("Failed to close handle: %v", err)
	}
}

func TestHandleGetLastErrorOnSuccess(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	msg, err := h.GetLastError()
	assert.Equal(t, "", msg)
	assert.NoError(t, err)
}

func TestHandleAPIVersionNotEmpty(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.NotEmpty(t, h.APIVersion())
}

func TestHandleAPIBuildNotEmpty(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.NotEmpty(t, h.APIBuild())
}

func TestHandleSetTimeoutValid(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.NoError(t, h.SetTimeout(testTimeout))
}

func TestHandleSetTimeoutZero(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.Error(t, h.SetTimeout(0))
}

func TestHandleSetMaxCardinalityValid(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.NoError(t, h.SetMaxCardinality(10007))
}

func TestHandleSetMaxCardinalityTooSmall(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.Error(t, h.SetMaxCardinality(99))
}

func TestHandleSetCompressionRandomValueFails(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.Error(t, h.SetCompression(5))
}

func TestHandleSetClientMaxInBufSizeValid(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.NoError(t, h.SetClientMaxInBufSize(100000000))
}

func TestHandleSetClientMaxInBufSizeTooSmall(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	assert.Error(t, h.SetClientMaxInBufSize(100))
}

func TestHandleGetClientMaxInBufSize(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	require.NoError(t, h.SetClientMaxInBufSize(100000000))
	v, err := h.GetClientMaxInBufSize()
	require.NoError(t, err)
	assert.Equal(t, uint(100000000), uint(v))
}

func TestHandleGetClusterMaxInBufSize(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	v, err := h.GetClusterMaxInBufSize()
	require.NoError(t, err)
	assert.NotZero(t, v)
}

func TestHandleGetClientMaxParallelism(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	v, err := h.GetClientMaxParallelism()
	require.NoError(t, err)
	assert.NotZero(t, v)
}

//----------------------------------------------------
// Tag and prefix helpers (requires an entry)
//----------------------------------------------------

func TestHandleTagsAndPrefixFunctions(t *testing.T) {
	h := newTestHandle(t)
	defer func() {
		err := h.Close()
		if err != nil {
			t.Errorf("Failed to close h: %v", err)
		}
	}()

	alias := generateAlias(16)
	integer := h.Integer(alias)
	require.NoError(t, integer.Put(8, NeverExpires()))
	defer func() {
		err := integer.Remove()
		if err != nil {
			t.Errorf("Failed to remove integer: %v", err)
		}
	}()

	// no tags initially
	tagList, err := h.GetTags(alias)
	require.NoError(t, err)
	assert.Empty(t, tagList)

	// bad alias
	_, err = h.GetTags("")
	assert.Error(t, err)

	// attach a tag and verify
	require.NoError(t, integer.AttachTag("tag"))
	got, err := h.GetTags(alias)
	require.NoError(t, err)
	assert.Equal(t, []string{"tag"}, got)

	// GetTagged
	list, err := h.GetTagged("tag")
	require.NoError(t, err)
	assert.Equal(t, []string{alias}, list)

	// GetTagged with missing tag
	list, err = h.GetTagged("missing")
	require.NoError(t, err)
	assert.Empty(t, list)

	// PrefixGet / PrefixCount with wrong prefix
	prefix := alias[:3]
	_, err = h.PrefixGet("bad", 10)
	assert.ErrorIs(t, err, ErrAliasNotFound)

	// correct prefix
	entries, err := h.PrefixGet(prefix, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{alias}, entries)

	// PrefixCount
	count, err := h.PrefixCount("bad")
	require.NoError(t, err)
	assert.Zero(t, count)

	count, err = h.PrefixCount(prefix)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}
