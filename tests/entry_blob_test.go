package qdbtests

import (
	"bytes"
	"testing"

	. "github.com/bureau14/qdb-api-go"
)

// TestConnect testing various things about connection
func TestBlobEntry(t *testing.T) {
	handle, err := setupHandle()
	if err != nil {
		t.Error("Setup failed: ", err)
	}

	alias := generateAlias(16)
	content := []byte("content")
	blob := handle.Blob(alias)

	// Test BlobPut
	err = blob.Put(content, NeverExpires())
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	err = blob.Put(content, NeverExpires())
	if err == nil {
		t.Error("Expected error on BlobPut with already used alias - got nil")
	}

	// Test update
	newContent := []byte("newContent")
	err = blob.Update(newContent, NeverExpires())
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}

	// Test Get
	var contentObtained []byte
	contentObtained, err = blob.Get()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if bytes.Equal(contentObtained, newContent) == false {
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
	if bytes.Equal(contentObtained, []byte{}) == false {
		t.Error("Expected contentObtained to be [] got: ", contentObtained)
	}
}
