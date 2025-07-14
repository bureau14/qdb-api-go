package qdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegerPut(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)
	defer integer.Remove()

	content := int64(13)
	err := integer.Put(content, NeverExpires())
	assert.NoError(t, err)
}

func TestIntegerPutAgain(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)
	defer integer.Remove()

	content := int64(13)
	require.NoError(t, integer.Put(content, NeverExpires()))
	err := integer.Put(content, NeverExpires())
	assert.Error(t, err)
}

func TestIntegerUpdate(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)
	defer integer.Remove()

	content := int64(13)
	newContent := int64(87)

	require.NoError(t, integer.Put(content, NeverExpires()))
	require.NoError(t, integer.Update(newContent, NeverExpires()))

	got, err := integer.Get()
	require.NoError(t, err)
	assert.Equal(t, newContent, got)
}

func TestIntegerGet(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)
	defer integer.Remove()

	content := int64(13)
	require.NoError(t, integer.Put(content, NeverExpires()))

	got, err := integer.Get()
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestIntegerAdd(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)
	defer integer.Remove()

	content := int64(13)
	require.NoError(t, integer.Put(content, NeverExpires()))

	toAdd := int64(5)
	expected := content + toAdd

	sum, err := integer.Add(toAdd)
	require.NoError(t, err)
	assert.Equal(t, expected, sum)
}

func TestIntegerRemove(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	integer := handle.Integer(alias)

	content := int64(13)
	require.NoError(t, integer.Put(content, NeverExpires()))

	require.NoError(t, integer.Remove())

	_, err := integer.Get()
	assert.Error(t, err)
}
