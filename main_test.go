package qdb

import (
	"fmt"
	"os"
	"testing"
)

func setupHandle() (HandleType, error) {
	handle, err := NewHandle()
	qdbConnection := fmt.Sprintf("qdb://%s:%s", getenv("QDB_HOST", "127.0.0.1"), getenv("QDB_PORT", "2836"))
	err = handle.Connect(qdbConnection)
	return handle, err
}

func MustSetupHandle() HandleType {
	handle, err := NewHandle()
	qdbConnection := fmt.Sprintf("qdb://%s:%s", getenv("QDB_HOST", "127.0.0.1"), getenv("QDB_PORT", "2836"))
	err = handle.Connect(qdbConnection)
	if err != nil {
		panic(err)
	}
	return handle
}

func TestMain(m *testing.M) {
	retCode := m.Run()
	os.Exit(retCode)
}
