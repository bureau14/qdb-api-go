package qdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeStatusWithEmptyURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, "")
	_, err := node.Status()
	assert.Error(t, err)
}

func TestNodeStatusWithInvalidURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, "qdb://127.0.0.1:36321")
	_, err := node.Status()
	assert.Error(t, err)
}

func TestNodeStatusWithValidURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, insecureURI)
	status, err := node.Status()
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:2836", status.Network.ListeningEndpoint)
}

func TestNodeConfigWithEmptyURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, "")
	_, err := node.Config()
	assert.Error(t, err)
}

func TestNodeConfigWithInvalidURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, "qdb://127.0.0.1:36321")
	_, err := node.Config()
	assert.Error(t, err)
}

func TestNodeConfigWithValidURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, insecureURI)
	configBytes, err := node.Config()
	require.NoError(t, err)

	config, err := parseJSON(configBytes)
	require.NoError(t, err)

	rootPath := config.Path("local.depot.rocksdb.root").Data().(string)
	assert.Equal(t, "insecure/db/0-0-0-1", rootPath)
}

func TestNodeTopologyWithEmptyURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, "")
	_, err := node.Topology()
	assert.Error(t, err)
}

func TestNodeTopologyWithInvalidURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, "qdb://127.0.0.1:36321")
	_, err := node.Topology()
	assert.Error(t, err)
}

func TestNodeTopologyWithValidURI(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	node := newTestNode(handle, insecureURI)
	topology, err := node.Topology()
	require.NoError(t, err)
	assert.Equal(t, topology.Predecessor.Endpoint, topology.Successor.Endpoint)
}
