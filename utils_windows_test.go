// +build windows

package qdb

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

func mkBinaryPath(path string, binary string) string {
	return fmt.Sprintf("%s.exe", filepath.Join(path, binary))
}

func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

func (d *db) setInfo(s Security) {
	d.config = "qdb"
	d.data = "db"
	d.port = qdbPort
	d.bin = "test_qdbd"
	if s == SecurityEnabled {
		d.config += "_secured"
		d.bin += "_secured"
		d.data += "_secured"
		d.port++
	}
	if s == SecurityEncrypted {
		d.config += "_encrypted"
		d.bin += "_encrypted"
		d.data += "_encrypted"
		d.port++
	}
	d.bin += ".exe"
	d.config += ".cfg"
}

func (d db) stop() error {
	_, err := exec.Command("taskkill", "/F", "/T", "/IM", d.bin).Output()
	return err
}
