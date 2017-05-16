package qdb

// Name of file is to make it appear close to entry.go

import "testing"

// Testing entry via integer entry type
func TestEntry(t *testing.T) {
	handle, err := setupHandle()
	if err != nil {
		t.Error("Setup failed: ", err)
	}

	alias := generateAlias(16)
	content := int64(13)
	integer := handle.Integer(alias)

	integer.Put(content, NeverExpires)

	err = integer.AttachTag("")
	if err == nil {
		t.Error("Expected an error - got nil")
	}

	// Test Attach tag
	tag := generateAlias(5)
	err = integer.AttachTag(tag)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	// Test Has tag
	err = integer.HasTag(tag)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	var aliases []string
	// Test Get tagged
	aliases, err = integer.GetTagged(tag)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if len(aliases) != 1 {
		t.Error("Expected len(aliases) to be one got: ", len(aliases))
	}
	if aliases[0] != alias {
		t.Error("Alias is ", aliases[0], " should be ", alias)
	}
	// Test Get tags
	var tags []string
	tags, err = integer.GetTags()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if len(tags) != 1 {
		t.Error("Expected len(tags) to be one got: ", len(tags))
	}
	if tags[0] != tag {
		t.Error("Tag is ", tags[0], " should be ", tag)
	}

	// Test Detach tag
	err = integer.DetachTag(tag)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	err = integer.HasTag(tag)
	if err == nil {
		t.Error("Expected an error - got nil")
	}
	// Test Remove
	err = integer.Remove()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	_, err = integer.Get()
	if err == nil {
		t.Error("Expected an error - got nil")
	}
}
