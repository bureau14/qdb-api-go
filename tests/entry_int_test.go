package qdbtests

import (
	"testing"

	. "github.com/bureau14/qdb-api-go"
)

// TestConnect testing various things about connection
func TestIntegerEntry(t *testing.T) {
	handle, err := setupHandle()
	if err != nil {
		t.Error("Setup failed: ", err)
	}

	alias := generateAlias(16)
	content := int64(13)
	integer := handle.Integer(alias)

	// Test IntegerPut
	err = integer.Put(content, NeverExpires())
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	err = integer.Put(content, NeverExpires())
	if err == nil {
		t.Error("Expected error on IntegerPut with already used alias - got nil")
	}

	// Test update
	newContent := int64(87)
	err = integer.Update(newContent, NeverExpires())
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}

	// Test Get
	var contentObtained int64
	contentObtained, err = integer.Get()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if contentObtained != newContent {
		t.Error("Expected contentObtained should be ", newContent, " got: ", contentObtained)
	}

	// Test Remove
	err = integer.Remove()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	contentObtained, err = integer.Get()
	if err == nil {
		t.Error("Expected error on BlobGet after deleting data - got nil")
	}
	if contentObtained != 0 {
		t.Error("Expected contentObtained to be nil got: ", contentObtained)
	}
}
