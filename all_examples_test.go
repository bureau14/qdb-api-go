package qdb

import (
	"fmt"
)

func ExampleHandleType() {
	var handle HandleType
	handle.Open(ProtocolTCP)
	fmt.Printf("API build: %s\n", handle.APIVersion())
	// Output: API build: 2.1.0master
}

func ExampleEntry_Alias() {
	handle := CreateExampleHandle()
	defer handle.Close()

	alias := "EntryAlias"
	blob := handle.Blob(alias)

	fmt.Printf("Alias: %s\n", blob.Alias())
	// Output: Alias: EntryAlias
}

func ExampleBlobEntry_Put() {
	handle := CreateExampleHandle()
	defer handle.Close()

	blob := handle.Blob("blob")
	blob.Put([]byte("content"), NeverExpires())

	content, _ := blob.Get()
	fmt.Printf("Content: %s", content)
	// Output: Content: content
}
