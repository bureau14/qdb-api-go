package qdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlobPutEmpty tests putting empty content into a blob
func TestBlobPutEmpty(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	blob, err := newTestBlobWithContent(t, handle, []byte{})
	require.NoError(t, err)
	defer blob.Remove()

	contentObtained, err := blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, contentObtained)
}

// TestBlobUpdateEmpty tests updating a blob with empty content
func TestBlobUpdateEmpty(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	blob := handle.Blob(alias)
	defer blob.Remove()

	err := blob.Update([]byte{}, NeverExpires())
	assert.NoError(t, err)

	contentObtained, err := blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, contentObtained)
}

// TestBlobGetEmpty tests getting empty blob content
func TestBlobGetEmpty(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	blob, err := newTestBlobWithContent(t, handle, []byte{})
	require.NoError(t, err)
	defer blob.Remove()

	contentObtained, err := blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, contentObtained)
}

// TestBlobGetAndUpdateEmpty tests get and update operation with empty initial content
func TestBlobGetAndUpdateEmpty(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	blob, err := newTestBlobWithContent(t, handle, []byte{})
	require.NoError(t, err)
	defer blob.Remove()

	newContent := []byte("newContent")
	contentObtained, err := blob.GetAndUpdate(newContent, PreserveExpiration())
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, contentObtained)

	contentObtained, err = blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, newContent, contentObtained)
}

// TestBlobGetAndRemoveEmpty tests get and remove operation with empty content
func TestBlobGetAndRemoveEmpty(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	blob, err := newTestBlobWithContent(t, handle, []byte{})
	require.NoError(t, err)
	// No defer Remove() since we're removing it in the test

	contentObtained, err := blob.GetAndRemove()
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, contentObtained)

	contentObtained, err = blob.Get()
	assert.Error(t, err)
	assert.Equal(t, []byte{}, contentObtained)
}

// TestBlobRemoveIfEmpty tests conditional remove with empty content
func TestBlobRemoveIfEmpty(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	blob, err := newTestBlobWithContent(t, handle, []byte{})
	require.NoError(t, err)
	// No defer Remove() since we're removing it in the test

	err = blob.RemoveIf([]byte{})
	assert.NoError(t, err)

	contentObtained, err := blob.Get()
	assert.Error(t, err)
	assert.Equal(t, []byte{}, contentObtained)
}

// TestBlobRemoveIfEmptyWithBadComparand tests conditional remove with wrong comparand on empty blob
func TestBlobRemoveIfEmptyWithBadComparand(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	blob, err := newTestBlobWithContent(t, handle, []byte{})
	require.NoError(t, err)
	defer blob.Remove()

	err = blob.RemoveIf([]byte("badContent"))
	assert.Error(t, err)
}

// TestBlobCompareAndSwapEmptyWithGoodComparand tests CAS with correct comparand on empty blob
func TestBlobCompareAndSwapEmptyWithGoodComparand(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	blob, err := newTestBlobWithContent(t, handle, []byte{})
	require.NoError(t, err)
	defer blob.Remove()

	newContent := []byte("newContent")
	contentObtained, err := blob.CompareAndSwap(newContent, []byte{}, PreserveExpiration())
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, contentObtained)

	contentObtained, err = blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, newContent, contentObtained)
}

// TestBlobCompareAndSwapEmptyWithBadComparand tests CAS with wrong comparand on empty blob
func TestBlobCompareAndSwapEmptyWithBadComparand(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	blob, err := newTestBlobWithContent(t, handle, []byte{})
	require.NoError(t, err)
	defer blob.Remove()

	newContent := []byte("newContent")
	contentObtained, err := blob.CompareAndSwap(newContent, []byte("badContent"), PreserveExpiration())
	assert.Error(t, err)
	assert.Equal(t, []byte{}, contentObtained)
}

// TestBlobPutNormal tests putting normal content into a blob
func TestBlobPutNormal(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	contentObtained, err := blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, content, contentObtained)
}

// TestBlobUpdateNormal tests updating a blob with normal content
func TestBlobUpdateNormal(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	alias := generateAlias(16)
	blob := handle.Blob(alias)
	defer blob.Remove()

	content := []byte("content")
	err := blob.Update(content, NeverExpires())
	assert.NoError(t, err)

	contentObtained, err := blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, content, contentObtained)
}

// TestBlobGetNormal tests getting normal blob content
func TestBlobGetNormal(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	contentObtained, err := blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, content, contentObtained)
}

// TestBlobGetAndUpdateNormal tests get and update operation with normal content
func TestBlobGetAndUpdateNormal(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	newContent := []byte{}
	contentObtained, err := blob.GetAndUpdate(newContent, PreserveExpiration())
	assert.NoError(t, err)
	assert.Equal(t, content, contentObtained)

	contentObtained, err = blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, newContent, contentObtained)
}

// TestBlobGetAndRemoveNormal tests get and remove operation with normal content
func TestBlobGetAndRemoveNormal(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	// No defer Remove() since we're removing it in the test

	contentObtained, err := blob.GetAndRemove()
	assert.NoError(t, err)
	assert.Equal(t, content, contentObtained)

	contentObtained, err = blob.Get()
	assert.Error(t, err)
	assert.Equal(t, []byte{}, contentObtained)
}

// TestBlobRemoveIfNormal tests conditional remove with correct comparand
func TestBlobRemoveIfNormal(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	// No defer Remove() since we're removing it in the test

	err = blob.RemoveIf(content)
	assert.NoError(t, err)

	contentObtained, err := blob.Get()
	assert.Error(t, err)
	assert.Equal(t, []byte{}, contentObtained)
}

// TestBlobRemoveIfNormalWithBadComparand tests conditional remove with wrong comparand
func TestBlobRemoveIfNormalWithBadComparand(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	err = blob.RemoveIf([]byte("badContent"))
	assert.Error(t, err)
}

// TestBlobCompareAndSwapNormalWithGoodComparand tests CAS with correct comparand
func TestBlobCompareAndSwapNormalWithGoodComparand(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	newContent := []byte{}
	contentObtained, err := blob.CompareAndSwap(newContent, content, PreserveExpiration())
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, contentObtained)

	contentObtained, err = blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, newContent, contentObtained)
}

// TestBlobCompareAndSwapNormalWithBadComparand tests CAS with wrong comparand
func TestBlobCompareAndSwapNormalWithBadComparand(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	newContent := []byte{}
	contentObtained, err := blob.CompareAndSwap(newContent, []byte("badContent"), PreserveExpiration())
	assert.Error(t, err)
	assert.Equal(t, content, contentObtained)
}

// TestBlobGetNoAllocExactBuffer tests GetNoAlloc with exact buffer size
func TestBlobGetNoAllocExactBuffer(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	exactbuf := make([]byte, len(content))
	n, err := blob.GetNoAlloc(exactbuf)
	assert.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, exactbuf)
}

// TestBlobGetNoAllocSmallBuffer tests GetNoAlloc with buffer too small
func TestBlobGetNoAllocSmallBuffer(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	smallbuf := make([]byte, len(content)-2)
	n, err := blob.GetNoAlloc(smallbuf)
	assert.Error(t, err)
	assert.Equal(t, len(content), n)
}

// TestBlobGetNoAllocBigBuffer tests GetNoAlloc with buffer larger than content
func TestBlobGetNoAllocBigBuffer(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	bigbuf := make([]byte, len(content)+2)
	n, err := blob.GetNoAlloc(bigbuf)
	assert.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, bigbuf[:n])
}
