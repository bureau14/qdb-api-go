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
