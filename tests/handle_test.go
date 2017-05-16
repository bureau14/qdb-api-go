package qdbtests

import (
	"os"
	"testing"
	. "qdb"
)

func connectWithoutCreatingHandle(t *testing.T, handle *HandleType, err error) {
	err = handle.Connect("")
	if err == nil {
		t.Error("Expected error on: Connect without a proper handle - got nil")
	}
}

func connectWithoutAddress(t *testing.T, handle *HandleType, err error) {
	*handle, err = NewHandle()
	err = handle.Connect("")
	if err == nil {
		t.Error("Expected error on: Connect without a proper address - got nil")
	}
}

func connectProperly(t *testing.T, handle *HandleType, err error) {
	qdbConnection := string("qdb://127.0.0.1:")
	qdbConnection += os.Args[1]
	err = handle.Connect(qdbConnection)
	if err != nil {
		t.Error("Expected no error, got ", err)
	}
}

// TestHandle testing various things about connection
func TestHandle(t *testing.T) {
	var handle HandleType
	var err error
	connectWithoutCreatingHandle(t, &handle, err)
	connectWithoutAddress(t, &handle, err)
	connectProperly(t, &handle, err)
}
