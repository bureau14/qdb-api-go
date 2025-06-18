package qdb

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestDirectPrefixGet(t *testing.T) {
	dh := newTestDirectHandle(t)
	entries, err := dh.PrefixGet("$qdb", 1000)
	require.NoError(t, err)
	require.Greater(t, len(entries), 0)
}

func TestDirectBlobEntryPutIsNotImplemented(t *testing.T) {
	t.Skip("C API not yet implemented")
}

func TestDirectIntegerEntryPutIsNotImplemented(t *testing.T) {
	t.Skip("C API not yet implemented")
}
