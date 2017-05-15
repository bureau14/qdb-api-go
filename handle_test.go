package qdb

import (
	"os"
	"testing"
)

// TestConnect testing various things about connection
func TestHandle(t *testing.T) {
	var handle HandleType
	err := handle.Connect("")
	if err == nil {
		t.Error("Expected error on: Connect without a proper handle - got nil")
	}
	handle, err = NewHandle()
	err = handle.Connect("")
	if err == nil {
		t.Error("Expected error on: Connect without a proper address - got nil")
	}
	qdbConnection := string("qdb://127.0.0.1:")
	qdbConnection += os.Args[1]
	err = handle.Connect(qdbConnection)
	if err != nil {
		t.Error("Expected no error, got ", err)
	}
}
