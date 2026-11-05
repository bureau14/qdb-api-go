package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatisticsEngineVersion(t *testing.T) {
	handle := newTestHandle(t)

	// allow the daemon to gather statistics
	time.Sleep(5 * time.Second)

	results, err := handle.Statistics()
	require.NoError(t, err)

	for _, r := range results {
		require.GreaterOrEqual(t, len(r.EngineVersion), 2)
		require.Equal(t, "3.", r.EngineVersion[:2])
	}
}

func TestNodeStatisticsMatchesStatistics(t *testing.T) {
	handle := newTestHandle(t)

	endpoints, err := handle.Cluster().Endpoints()
	require.NoError(t, err)
	require.NotEmpty(t, endpoints)

	all, err := handle.Statistics()
	require.NoError(t, err)

	for _, e := range endpoints {
		stats, err := handle.NodeStatistics(e.URI())
		require.NoError(t, err)
		require.Contains(t, all, stats.NodeID)
		require.Equal(t, all[stats.NodeID].EngineVersion, stats.EngineVersion)
	}
}
