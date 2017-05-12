package qdb

import (
	"testing"
)

// TestConnect testing various things about connection
func TestBlob(t *testing.T) {
	handle, err := NewHandle()
	err = handle.Connect("qdb://127.0.0.1:2836")

	alias := generateAlias(16)

	content := string("content")
	// Test BlobPut
	err = handle.BlobPut(alias, content, NeverExpires)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	err = handle.BlobPut(alias, content, NeverExpires)
	if err == nil {
		t.Error("Expected error on BlobPut with already used alias - got nil")
	}

	// Test update
	newContent := string("newContent")
	err = handle.BlobUpdate(alias, newContent, NeverExpires)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}

	// Test Get
	var contentObtained string
	contentObtained, err = handle.BlobGet(alias)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if contentObtained != newContent {
		t.Error("Expected contentObtained should be ", newContent, " got: ", contentObtained)
	}

	// Test Remove
	err = handle.BlobRemove(alias)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	contentObtained, err = handle.BlobGet(alias)
	if err == nil {
		t.Error("Expected error on BlobGet after deleting data - got nil")
	}
	if contentObtained != "" {
		t.Error("Expected contentObtained to be \"\" got: ", contentObtained)
	}
}
