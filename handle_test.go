package qdb

import (
	"fmt"
	"testing"
)

// TestHandle testing various things about connection
func TestHandle(t *testing.T) {
	var handle HandleType
	var err error

	testConnection(t, &handle)

	miscelaneous(t, &handle, err)
}

func testConnection(t *testing.T, handle *HandleType) {
	qdbConnection := fmt.Sprintf("qdb://%s:%s", getenv("QDB_HOST", "127.0.0.1"), getenv("QDB_PORT", "2836"))
	err := handle.Connect(qdbConnection)
	if err == nil {
		t.Error("Should not be able to connect without creating handle")
	}
	*handle, err = NewHandle()
	err = handle.Connect("")
	if err == nil {
		t.Error("Should not be able to connect without address")
	}
	err = handle.Connect(qdbConnection)
	if err != nil {
		t.Error("Should be able to connect properly after creating a handle")
	}
	err = handle.Close()
	if err != nil {
		t.Error("Should be able to close properly")
	}
	err = handle.Open(2)
	if err == nil {
		t.Error("Should not be able to open with random protocol")
	}
	err = handle.Open(ProtocolTCP)
	if err != nil {
		t.Error("Should be able to open with TCP protocol")
	}
	err = handle.Connect(qdbConnection)
	if err != nil {
		t.Error("Should be able to connect properly opening a handle")
	}
}

func miscelaneous(t *testing.T, handle *HandleType, err error) {
	if handle.APIVersion() == "" {
		t.Error("Should not return an empty version")
	}
	if handle.APIBuild() == "" {
		t.Error("Should not return an empty version")
	}
	err = handle.SetTimeout(1000)
	if err != nil {
		t.Error("Should be able set timeout to 1s.")
	}
	err = handle.SetTimeout(0)
	if err == nil {
		t.Error("Should not be able set timeout to 0ms.")
	}
	err = handle.SetMaxCardinality(10007)
	if err != nil {
		t.Error("Should be able to call SetMaxCardinality with default value.")
	}
	err = handle.SetMaxCardinality(99)
	if err == nil {
		t.Error("Should not be able to call SetMaxCardinality with value under 100.")
	}
	err = handle.SetCompression(CompFast)
	if err != nil {
		t.Error("Should be able to set compression to best.")
	}
	err = handle.SetCompression(5)
	if err == nil {
		t.Error("Should not be able to set compression with random value.")
	}
}

func version(t *testing.T, handle *HandleType) {
}

func build(t *testing.T, handle *HandleType) {
}
