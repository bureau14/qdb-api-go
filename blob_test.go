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
	err = handle.BlobPutSimple(alias, content)
	if err == nil {
		t.Error("Expected error on BlobPutSimple with already used alias - got nil")
	}
	var contentObtained string
	contentObtained, err = handle.BlobGetAndRemove(alias)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if contentObtained != content {
		t.Error("Expected contentObtained == \"content\" got: ", contentObtained)
	}
	contentObtained, err = handle.BlobGet(alias)
	if err == nil {
		t.Error("Expected error on BlobGet after deleting data - got nil")
	}
	if contentObtained != "" {
		t.Error("Expected contentObtained to be nil got: ", contentObtained)
	}
}
