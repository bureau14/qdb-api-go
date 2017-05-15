package qdb

import (
	"bytes"
	"os"
	"testing"
)

func setup() (HandleType, error) {
	handle, err := NewHandle()
	qdbConnection := string("qdb://127.0.0.1:")
	qdbConnection += os.Args[1]
	err = handle.Connect(qdbConnection)
	return handle, err
}

// TestConnect testing various things about connection
func TestBlob(t *testing.T) {
	handle, err := setup()
	if err != nil {
		t.Error("Setup failed: ", err)
	}

	alias := generateAlias(16)
	content := []byte("content")
	blob := NewBlob(handle, NeverExpires, alias, content)

	// Test BlobPut
	err = blob.Put()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	err = blob.Put()
	if err == nil {
		t.Error("Expected error on BlobPut with already used alias - got nil")
	}

	// Test update
	newContent := []byte("newContent")
	err = blob.Update(newContent, NeverExpires)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}

	// Test Get
	var contentObtained []byte
	contentObtained, err = blob.Get()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if bytes.Equal(contentObtained, newContent) == true {
		t.Error("Expected contentObtained should be ", newContent, " got: ", contentObtained)
	}

	// Test Remove
	err = blob.Remove()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	contentObtained, err = blob.Get()
	if err == nil {
		t.Error("Expected error on BlobGet after deleting data - got nil")
	}
	if contentObtained != nil {
		t.Error("Expected contentObtained to be nil got: ", contentObtained)
	}
}
