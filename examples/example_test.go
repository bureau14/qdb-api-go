package qdbtests

import (
	"bytes"
	"fmt"
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
	fmt.Println("\nCreate a blob with `blob := handle.Blob('yourAlias')`\n")
	blob := handle.Blob(alias)

	content := []byte("yourContent")
	fmt.Print("Put content with `blob.Put('yourContent', expiryValue)`\n")
	err = blob.Put(content, NeverExpires)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	fmt.Print("You can't put a blob associated with an alias more than one time\n")
	fmt.Println("`blob.Put('yourContent', NeverExpires)` won't work if you call it again\n")
	err = blob.Put(content, NeverExpires)
	if err == nil {
		t.Error("Expected error on BlobPut with already used alias - got nil")
	}

	newContent := []byte("yourNewContent")
	fmt.Println("But you can update it with `blob.Update('yourNewContent', expiryValue)`\n")
	err = blob.Update(newContent, NeverExpires)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}

	var contentObtained []byte
	fmt.Print("Let's get our data: `contentObtained, err = blob.Get()`\n")
	contentObtained, err = blob.Get()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if bytes.Equal(contentObtained, newContent) == false {
		t.Error("Expected contentObtained should be ", newContent, " got: ", contentObtained)
	}
	fmt.Printf("data: %s\n\n", string(contentObtained))

	fmt.Print("Remove our data: `blob.Remove()`\n")
	err = blob.Remove()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	fmt.Print("We can no longer access our data via Get\n")
	contentObtained, err = blob.Get()
	if err == nil {
		t.Error("Expected error on BlobGet after deleting data - got nil")
	}
	if bytes.Equal(contentObtained, []byte{}) == false {
		t.Error("Expected contentObtained to be [] got: ", contentObtained)
	}
	fmt.Println("But we could Put again\n")
}
