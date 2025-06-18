package qdb

import (
	"testing"
	"time"
	"github.com/stretchr/testify/require"
)

func TestStatisticsEngineVersion(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	// allow the daemon to gather statistics
	time.Sleep(5 * time.Second)

	results, err := handle.Statistics()
	require.NoError(t, err)

	for _, r := range results {
		require.GreaterOrEqual(t, len(r.EngineVersion), 2)
		require.Equal(t, "3.", r.EngineVersion[:2])
	}
}
