// +build !windows

package qdb

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

func mkBinaryPath(path string, binary string) string {
	return filepath.Join(path, binary)
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
	os.Chmod(dst, 0744)
	return
}

func (d *db) setInfo(s Security) {
	d.config = "qdb"
	d.data = "db"
	d.port = qdbPort
	d.bin = "./test_qdbd"
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
	d.config += ".cfg"
}

func (d db) stop() error {
	_, err := exec.Command("killall", d.bin).Output()
	return err
}

func writeJsonToFile(path string, jsonObject interface{}) error {
	data, err := json.MarshalIndent(&jsonObject, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, data, 0744)
}
