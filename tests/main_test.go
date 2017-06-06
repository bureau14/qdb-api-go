package qdbtests

import (
	"fmt"
	"os"
	"testing"

	. "github.com/bureau14/qdb-api-go"
)

func setupHandle() (HandleType, error) {
	handle, err := NewHandle()
	qdbConnection := fmt.Sprintf("qdb://%s:%s", getenv("QDB_HOST", "127.0.0.1"), getenv("QDB_PORT", "2836"))
	err = handle.Connect(qdbConnection)
	return handle, err
}

func TestMain(m *testing.M) {
	retCode := m.Run()
	os.Exit(retCode)
}
