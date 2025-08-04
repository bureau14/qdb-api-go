package qdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ------ Find-query unit tests (Ginkgo/Gomega removed) ------

func TestFindTagAllReturnsAllAliases(t *testing.T) {
	handle := newTestHandle(t)

	aliases, _, _, _, tagAll, _, _, _ := setupFindTestData(t, handle)

	got, err := handle.Find().Tag(tagAll).Execute()
	require.NoError(t, err)
	assert.Len(t, got, 3)
	assert.ElementsMatch(t, aliases, got)
}

func TestFindExcludeSecondTag(t *testing.T) {
	handle := newTestHandle(t)

	_, blob1, _, integer, tagAll, _, tagSecond, _ := setupFindTestData(t, handle)

	got, err := handle.Find().Tag(tagAll).NotTag(tagSecond).Execute()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{blob1.Alias(), integer.Alias()}, got)
}

func TestFindNotTagAloneReturnsError(t *testing.T) {
	handle := newTestHandle(t)

	_, _, _, _, _, _, tagSecond, _ := setupFindTestData(t, handle)

	got, err := handle.Find().NotTag(tagSecond).Execute()
	assert.Error(t, err)
	assert.Empty(t, got)
}

func TestFindTagAllAndThird(t *testing.T) {
	handle := newTestHandle(t)

	_, _, _, integer, tagAll, _, _, tagThird := setupFindTestData(t, handle)

	got, err := handle.Find().Tag(tagAll).Tag(tagThird).Execute()
	require.NoError(t, err)
	assert.Equal(t, []string{integer.Alias()}, got)
}

func TestFindIncompatibleTagsReturnsEmpty(t *testing.T) {
	handle := newTestHandle(t)

	_, _, _, _, _, tagFirst, _, tagThird := setupFindTestData(t, handle)

	got, err := handle.Find().Tag(tagFirst).Tag(tagThird).Execute()
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestFindTagAllTypeBlob(t *testing.T) {
	handle := newTestHandle(t)

	_, blob1, blob2, _, tagAll, _, _, _ := setupFindTestData(t, handle)

	got, err := handle.Find().Tag(tagAll).Type("blob").Execute()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{blob1.Alias(), blob2.Alias()}, got)
}

func TestFindTypeBlobAloneReturnsError(t *testing.T) {
	handle := newTestHandle(t)

	setupFindTestData(t, handle)

	got, err := handle.Find().Type("blob").Execute()
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestFindTagAllTypeInt(t *testing.T) {
	handle := newTestHandle(t)

	_, _, _, integer, tagAll, _, _, _ := setupFindTestData(t, handle)

	got, err := handle.Find().Tag(tagAll).Type("int").Execute()
	require.NoError(t, err)
	assert.Equal(t, []string{integer.Alias()}, got)
}

func TestFindUnknownTagReturnsEmpty(t *testing.T) {
	handle := newTestHandle(t)

	setupFindTestData(t, handle)

	got, err := handle.Find().Tag("unexisting").Execute()
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestFindUnknownTypeReturnsError(t *testing.T) {
	handle := newTestHandle(t)

	_, _, _, _, tagAll, _, _, _ := setupFindTestData(t, handle)

	got, err := handle.Find().Tag(tagAll).Type("unexisting").Execute()
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestFindExecuteStringValidQuery(t *testing.T) {
	handle := newTestHandle(t)

	aliases, _, _, _, tagAll, _, _, _ := setupFindTestData(t, handle)

	got, err := handle.Find().ExecuteString(fmt.Sprintf("find(tag='%s')", tagAll))
	require.NoError(t, err)
	assert.ElementsMatch(t, aliases, got)
}
