package qdb

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// infosOf strips the typed column wrappers back to their column info.
func infosOf[T columnMetadata](cols []T) []TsColumnInfo {
	infos := make([]TsColumnInfo, len(cols))
	for i, c := range cols {
		infos[i] = TsColumnInfo{c.Name(), c.Type(), c.Symtable()}
	}

	return infos
}

// TestTableMetadataRoundTrip creates a random table, then checks every read
// path against what was created: the fetched and cached snapshots, the
// column lookup, the legacy column accessors, and cache invalidation after
// a column insert.
func TestTableMetadataRoundTrip(t *testing.T) {
	handle := newTestHandle(t)

	rapid.Check(t, func(rt *rapid.T) {
		columns := genTableColumns(rt)
		shardSize := genShardSize(rt)
		ttl := genTTL(rt)

		entry := handle.Timeseries(generateAlias(16))
		require.NoError(rt, entry.Create(shardSize, columns...))
		rt.Cleanup(func() { _ = entry.Remove() })
		require.NoError(rt, entry.SetTTL(ttl))
		want := columns

		m, err := entry.Metadata()
		require.NoError(rt, err)
		assert.Equal(rt, TableMetadata{Name: entry.Name(), Columns: want, ShardSize: shardSize, TTL: ttl}, m)

		cached, err := entry.CachedMetadata()
		require.NoError(rt, err)
		assert.Equal(rt, m, cached)

		for _, c := range want {
			got, ok := m.Column(c.Name())
			require.True(rt, ok, "column %q missing", c.Name())
			assert.Equal(rt, c, got)
		}
		_, ok := m.Column("no_such_column")
		assert.False(rt, ok)

		infos, err := entry.ColumnsInfo()
		require.NoError(rt, err)
		assert.Equal(rt, want, infos)
		blobs, doubles, int64s, strings, timestamps, err := entry.Columns()
		require.NoError(rt, err)
		assert.Equal(rt, columnInfosOfTypes(want, TsColumnBlob), infosOf(blobs))
		assert.Equal(rt, columnInfosOfTypes(want, TsColumnDouble), infosOf(doubles))
		assert.Equal(rt, columnInfosOfTypes(want, TsColumnInt64), infosOf(int64s))
		assert.Equal(rt, columnInfosOfTypes(want, TsColumnString, TsColumnSymbol), infosOf(strings))
		assert.Equal(rt, columnInfosOfTypes(want, TsColumnTimestamp), infosOf(timestamps))

		added := NewTsColumnInfo("added_col", TsColumnDouble)
		require.NoError(rt, entry.InsertColumns(added))
		cached, err = entry.CachedMetadata()
		require.NoError(rt, err)
		assert.Equal(rt, append(want, added), cached.Columns)
	})
}

func TestTableMetadataErrors(t *testing.T) {
	handle := newTestHandle(t)
	entry := handle.Timeseries(generateAlias(16))

	m, err := entry.Metadata()
	assert.True(t, errors.Is(err, ErrAliasNotFound), "got %v", err)
	assert.Equal(t, TableMetadata{}, m)

	m, err = entry.CachedMetadata()
	assert.True(t, errors.Is(err, ErrAliasNotFound), "got %v", err)
	assert.Equal(t, TableMetadata{}, m)

	err = entry.SetTTL(-time.Second)
	assert.True(t, errors.Is(err, ErrInvalidArgument), "got %v", err)
}

func TestHandleTableCacheTTL(t *testing.T) {
	base := NewHandleOptions()
	_, set := base.GetTableCacheTTL()
	assert.False(t, set)

	opts := base.WithTableCacheTTL(0)
	ttl, set := opts.GetTableCacheTTL()
	assert.True(t, set)
	assert.Equal(t, time.Duration(0), ttl)
	_, set = base.GetTableCacheTTL()
	assert.False(t, set, "WithTableCacheTTL must not mutate the receiver")

	for _, o := range []*HandleOptions{base, opts, base.WithTableCacheTTL(5 * time.Second)} {
		want, wantSet := o.GetTableCacheTTL()
		got, gotSet := FromHandleOptionsProvider(o).GetTableCacheTTL()
		assert.Equal(t, wantSet, gotSet, "FromHandleOptionsProvider must keep the set flag")
		assert.Equal(t, want, got, "FromHandleOptionsProvider must keep the TTL")
	}

	handle, err := NewHandleFromOptions(opts.WithClusterUri(insecureURI))
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })
	ttl, err = handle.GetTableCacheTTL()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), ttl, "option must reach the handle")

	rapid.Check(t, func(rt *rapid.T) {
		ttl := time.Duration(rapid.IntRange(0, math.MaxInt32/1000).Draw(rt, "cacheTTLSeconds")) * time.Second
		require.NoError(rt, handle.SetTableCacheTTL(ttl))
		got, err := handle.GetTableCacheTTL()
		require.NoError(rt, err)
		assert.Equal(rt, ttl, got)
	})

	err = handle.SetTableCacheTTL(-time.Second)
	assert.True(t, errors.Is(err, ErrInvalidArgument), "got %v", err)
	err = handle.SetTableCacheTTL(time.Duration(math.MaxInt32+1) * time.Millisecond)
	assert.True(t, errors.Is(err, ErrInvalidArgument), "got %v", err)
}
