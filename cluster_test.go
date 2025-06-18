package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterPurgeAll(t *testing.T) {
	// Note: This test requires secure handle setup which is not available in the current test infrastructure
	// Skipping the secure cluster test for now
	t.Skip("Secure handle tests require additional setup")
}

func TestClusterPurgeCache(t *testing.T) {
	// Note: This test requires secure handle setup which is not available in the current test infrastructure
	// Skipping the secure cluster test for now
	t.Skip("Secure handle tests require additional setup")
}

func TestClusterTrimAllWithBadHandle(t *testing.T) {
	h := HandleType{}
	c := h.Cluster()
	err := c.TrimAll()
	assert.Error(t, err)
}

func TestClusterTrimAllWithValidHandle(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	cluster := handle.Cluster()
	err := cluster.TrimAll()
	assert.NoError(t, err)
}

func TestClusterWaitForStabilizationWithBadHandle(t *testing.T) {
	h := HandleType{}
	c := h.Cluster()
	err := c.WaitForStabilization(60 * time.Second)
	assert.Error(t, err)
}

func TestClusterWaitForStabilizationWithValidHandle(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	cluster := handle.Cluster()
	err := cluster.WaitForStabilization(60 * time.Second)
	assert.NoError(t, err)
}

func TestClusterBlobExistsBeforeOperations(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content_blob")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	contentObtained, err := blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, content, contentObtained)
}

func TestClusterTrimAllWithExistingData(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	content := []byte("content_blob")
	blob, err := newTestBlobWithContent(t, handle, content)
	require.NoError(t, err)
	defer blob.Remove()

	cluster := handle.Cluster()
	err = cluster.TrimAll()
	assert.NoError(t, err)
	
	// Verify blob still exists after trim
	contentObtained, err := blob.Get()
	assert.NoError(t, err)
	assert.Equal(t, content, contentObtained)
}
