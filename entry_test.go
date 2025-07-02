package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ------------------------------------------------------------------
// Alias tests
// ------------------------------------------------------------------

func TestEntryAlias(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	assert.Equal(t, alias, integer.Alias())
}

func TestEntryPutWithEmptyAlias(t *testing.T) {
	handle := newTestHandle(t)
	integer := handle.Integer("")
	err := integer.Put(17, NeverExpires())
	defer handle.Close()

	assert.Error(t, err)
}

// ------------------------------------------------------------------
// Tag tests
// ------------------------------------------------------------------

func TestEntryAttachTag(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	assert.NoError(t, integer.AttachTag("atag"))
}

func TestEntryAttachTags(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)
	tags := generateTags(5)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	assert.NoError(t, integer.AttachTags(tags))
}

func TestEntryGetTagsWithoutAny(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	got, err := integer.GetTags()
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestEntryGetTagsAfterRemove(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	require.NoError(t, integer.Remove())
	defer handle.Close()

	got, err := integer.GetTags()
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestEntryTagLifecycle(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)
	tags := generateTags(5)
	tag := tags[0]

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	require.NoError(t, integer.AttachTags(tags))

	// Detach/HasTag
	assert.NoError(t, integer.HasTag(tag))
	assert.NoError(t, integer.DetachTag(tag))

	// Re-attach to test GetTagged and DetachTags
	require.NoError(t, integer.AttachTags(tags))

	aliases, err := integer.GetTagged(tag)
	require.NoError(t, err)

	// The server may return the same alias multiple times; de-duplicate.
	unique := make(map[string]struct{}, len(aliases))
	for _, a := range aliases {
		unique[a] = struct{}{}
	}
	require.Len(t, unique, 1, "expected exactly one unique alias for the tag")
	_, ok := unique[alias]
	assert.True(t, ok, "alias missing from GetTagged result")

	gotTags, err := integer.GetTags()
	require.NoError(t, err)
	assert.ElementsMatch(t, tags, gotTags)

	assert.NoError(t, integer.DetachTags(tags))
}

func TestEntryTagEdgeCases(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	assert.Error(t, integer.HasTag(""))
	aliases, err := integer.GetTagged("")
	assert.Error(t, err)
	assert.Empty(t, aliases)

	aliases, err = integer.GetTagged("nonexistent")
	require.NoError(t, err)
	assert.Empty(t, aliases)
}

// ------------------------------------------------------------------
// Expiry tests
// ------------------------------------------------------------------

func TestEntryExpiryDistantFuture(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	expiry := time.Date(2040, time.January, 1, 0, 0, 0, 0, time.UTC)
	duration := time.Until(expiry)

	require.NoError(t, integer.ExpiresAt(expiry))
	meta, err := integer.GetMetadata()
	require.NoError(t, err)
	assert.Equal(t, toQdbTime(expiry), toQdbTime(meta.ExpiryTime))

	assert.NoError(t, integer.ExpiresFromNow(duration))
}

func TestEntryExpiryShortFuture(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	duration, _ := time.ParseDuration("1h")
	expiry := time.Now().Add(duration)

	require.NoError(t, integer.ExpiresAt(expiry))
	meta, err := integer.GetMetadata()
	require.NoError(t, err)
	assert.Equal(t, toQdbTime(expiry), toQdbTime(meta.ExpiryTime))

	assert.NoError(t, integer.ExpiresFromNow(duration))
}

func TestEntryExpiryPastErrors(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	for _, d := range []time.Duration{-1 * time.Hour, -5*time.Minute - 30*time.Second} {
		expiry := time.Now().Add(d)
		assert.Error(t, integer.ExpiresAt(expiry))
		assert.Error(t, integer.ExpiresFromNow(d))
	}
}

func TestEntryExpiryPreserve(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	duration, _ := time.ParseDuration("1h")
	expiry := time.Now().Add(duration)

	// Default: NeverExpires
	meta, err := integer.GetMetadata()
	require.NoError(t, err)
	assert.Equal(t, NeverExpires(), meta.ExpiryTime)

	require.NoError(t, integer.Update(12, expiry))
	require.NoError(t, integer.Update(14, PreserveExpiration()))

	meta, err = integer.GetMetadata()
	require.NoError(t, err)
	assert.Equal(t, toQdbTime(expiry), toQdbTime(meta.ExpiryTime))
}

// ------------------------------------------------------------------
// Location & metadata tests
// ------------------------------------------------------------------

func TestEntryGetLocation(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	loc, err := integer.GetLocation()
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", loc.Address)
	assert.Equal(t, int16(2836), loc.Port)
}

func TestEntryGetMetadata(t *testing.T) {
	handle := newTestHandle(t)
	alias := generateAlias(16)

	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(13, NeverExpires()))
	defer integer.Remove()
	defer handle.Close()

	meta, err := integer.GetMetadata()
	require.NoError(t, err)
	assert.Equal(t, EntryInteger, meta.Type)
}

// ------------------------------------------------------------------
// Exists tests
// ------------------------------------------------------------------

func TestEntryExistsReturnsTrue(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(42, NeverExpires()))
	defer integer.Remove()

	assert.True(t, integer.Exists())
}

func TestEntryExistsReturnsFalse(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)

	assert.False(t, integer.Exists())
}

func TestEntryExistsAfterRemove(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(42, NeverExpires()))
	
	assert.True(t, integer.Exists())
	
	require.NoError(t, integer.Remove())
	assert.False(t, integer.Exists())
}

func TestEntryExistsWithEmptyAlias(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	integer := handle.Integer("")
	assert.False(t, integer.Exists())
}

func TestEntryExistsWithSpecialCharacters(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	testCases := []struct {
		name  string
		alias string
	}{
		{"unicode", "测试_alias_🌟"},
		{"symbols", "alias!@#$%^&*()"},
		{"spaces", "alias with spaces"},
		{"dots", "alias.with.dots"},
		{"slashes", "alias/with/slashes"},
		{"long", generateAlias(1024)}, // Test long alias
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			integer := handle.Integer(tc.alias)
			// Should not exist initially
			assert.False(t, integer.Exists())
			
			// Try to create entry - this may fail for some special characters
			err := integer.Put(42, NeverExpires())
			if err == nil {
				defer integer.Remove()
				assert.True(t, integer.Exists())
			} else {
				// If put fails, entry should still not exist
				assert.False(t, integer.Exists())
			}
		})
	}
}

func TestEntryExistsWithDifferentEntryTypes(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	testCases := []struct {
		name    string
		setup   func(string) (Entry, error)
		cleanup func(Entry)
	}{
		{
			name: "blob",
			setup: func(alias string) (Entry, error) {
				blob := handle.Blob(alias)
				err := blob.Put([]byte("test content"), NeverExpires())
				return blob.Entry, err
			},
			cleanup: func(e Entry) { e.Remove() },
		},
		{
			name: "integer",
			setup: func(alias string) (Entry, error) {
				integer := handle.Integer(alias)
				err := integer.Put(42, NeverExpires())
				return integer.Entry, err
			},
			cleanup: func(e Entry) { e.Remove() },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			alias := generateAlias(16)
			entry, err := tc.setup(alias)
			require.NoError(t, err)
			defer tc.cleanup(entry)

			assert.True(t, entry.Exists())
		})
	}
}

func TestEntryExistsConsistencyWithGetMetadata(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)

	// Before creation
	exists := integer.Exists()
	_, err := integer.GetMetadata()
	assert.Equal(t, err == nil, exists, "Exists() should match GetMetadata() error state")

	// After creation
	require.NoError(t, integer.Put(42, NeverExpires()))
	defer integer.Remove()

	exists = integer.Exists()
	_, err = integer.GetMetadata()
	assert.Equal(t, err == nil, exists, "Exists() should match GetMetadata() error state")
}

func TestEntryExistsConcurrentAccess(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)
	require.NoError(t, integer.Put(42, NeverExpires()))
	defer integer.Remove()

	// Test concurrent access to Exists() - should be safe
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			assert.True(t, integer.Exists())
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
