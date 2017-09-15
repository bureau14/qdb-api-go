// +build !windows

package qdb

import (
	"os"
	"path/filepath"
)

func setBinaryRights(path string) {
	os.Chmod(path, 0744)
}

func mkBinaryPath(path string, binary string) string {
	return filepath.Join(path, binary)
}

func addBinarybase(path *string) {
	*path += "./test_qdbd"
}

func addBinaryExtension(path *string) {
}
