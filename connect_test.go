package qdb

import "testing"

// TestConnect testing various things about connection
func TestConnect(t *testing.T) {
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
	err = handle.Connect("qdb://127.0.0.1:2836")
	if err != nil {
		t.Error("Expected no error, got ", err)
	}
}
