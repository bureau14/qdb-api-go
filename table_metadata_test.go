package qdb

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableMetadataReturnsColumnsShardSizeAndTTL(t *testing.T) {
	entry, expectedColumns := newTestTimeseriesMetadataTable(t)

	m, err := entry.Metadata()
	require.NoError(t, err)

	assert.Equal(t, entry.Name(), m.Name)
	assertColumnInfos(t, m.Columns, expectedColumns)
	assert.Equal(t, 24*time.Hour, m.ShardSize)
	assert.Equal(t, time.Duration(0), m.TTL)
}

func TestTableMetadataReflectsSetTTL(t *testing.T) {
	entry, _ := newTestTimeseriesMetadataTable(t)

	require.NoError(t, entry.SetTTL(48*time.Hour))

	m, err := entry.Metadata()
	require.NoError(t, err)
	assert.Equal(t, 48*time.Hour, m.TTL)

	require.NoError(t, entry.SetTTL(0))

	m, err = entry.Metadata()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), m.TTL)
}

func TestTableMetadataSetTTLRejectsNegative(t *testing.T) {
	entry, _ := newTestTimeseriesMetadataTable(t)

	err := entry.SetTTL(-time.Second)
	assert.True(t, errors.Is(err, ErrInvalidArgument), "got %v", err)
}

func TestTableMetadataColumnLookup(t *testing.T) {
	entry, expectedColumns := newTestTimeseriesMetadataTable(t)

	m, err := entry.Metadata()
	require.NoError(t, err)

	for _, want := range expectedColumns {
		got, ok := m.Column(want.Name())
		require.True(t, ok, "column %q missing", want.Name())
		assertColumnInfo(t, got, want)
	}
	_, ok := m.Column("no_such_column")
	assert.False(t, ok)
}

func TestTableMetadataReturnsAliasNotFound(t *testing.T) {
	handle := newTestHandle(t)
	entry := handle.Timeseries(generateAlias(16))

	m, err := entry.Metadata()
	assert.True(t, errors.Is(err, ErrAliasNotFound), "got %v", err)
	assert.Equal(t, TableMetadata{}, m)

	m, err = entry.CachedMetadata()
	assert.True(t, errors.Is(err, ErrAliasNotFound), "got %v", err)
	assert.Equal(t, TableMetadata{}, m)
}

func TestTableMetadataCachedMatchesFetched(t *testing.T) {
	entry, _ := newTestTimeseriesMetadataTable(t)

	fetched, err := entry.Metadata()
	require.NoError(t, err)
	cached, err := entry.CachedMetadata()
	require.NoError(t, err)

	assert.Equal(t, fetched, cached)
}

func TestTableMetadataCachedSeesColumnsInsertedOnSameHandle(t *testing.T) {
	entry, expectedColumns := newTestTimeseriesMetadataTable(t)

	_, err := entry.CachedMetadata()
	require.NoError(t, err)

	added := NewTsColumnInfo("added_col", TsColumnDouble)
	require.NoError(t, entry.InsertColumns(added))

	m, err := entry.CachedMetadata()
	require.NoError(t, err)
	assertColumnInfos(t, m.Columns, append(expectedColumns, added))
}

func TestHandleTableCacheTTLOption(t *testing.T) {
	handle := newTestHandle(t)

	ttl, err := handle.GetTableCacheTTL()
	require.NoError(t, err)
	assert.Equal(t, 60*time.Second, ttl)

	require.NoError(t, handle.SetTableCacheTTL(5*time.Second))
	ttl, err = handle.GetTableCacheTTL()
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, ttl)

	require.NoError(t, handle.SetTableCacheTTL(0))
	ttl, err = handle.GetTableCacheTTL()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), ttl)

	err = handle.SetTableCacheTTL(-time.Second)
	assert.True(t, errors.Is(err, ErrInvalidArgument), "got %v", err)
}

func TestHandleOptionsWithTableCacheTTL(t *testing.T) {
	base := NewHandleOptions()
	_, set := base.GetTableCacheTTL()
	assert.False(t, set)

	opts := base.WithTableCacheTTL(0)
	ttl, set := opts.GetTableCacheTTL()
	assert.True(t, set)
	assert.Equal(t, time.Duration(0), ttl)

	_, set = base.GetTableCacheTTL()
	assert.False(t, set, "WithTableCacheTTL must not mutate the receiver")

	handle, err := NewHandleFromOptions(opts.WithClusterUri(insecureURI))
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	ttl, err = handle.GetTableCacheTTL()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), ttl)
}
