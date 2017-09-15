package qdb

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

type db struct {
	bin    string
	data   string
	config string
	port   int
	exe    *exec.Cmd
}

func newDB(qdbd string, s Security) (*db, error) {
	d := &db{}
	d.setInfo(s)
	err := copyFile(qdbd, d.bin)
	if err != nil {
		return d, err
	}
	err = d.prepareConfig(s)
	return d, err
}

func (d db) prepareConfig(s Security) error {
	data, err := exec.Command(d.bin, "--gen-config").Output()
	if err != nil {
		return err
	}

	var nodeConfig NodeConfig
	err = json.Unmarshal(data, &nodeConfig)
	if err != nil {
		return err
	}

	nodeConfig.Local.Depot.Root = d.data
	nodeConfig.Global.Security.Enabled = false
	nodeConfig.Global.Security.EncryptTraffic = false
	if s == SecurityEnabled {
		nodeConfig.Global.Security.Enabled = true
		if s == SecurityEncrypted {
			nodeConfig.Global.Security.EncryptTraffic = true
		}
	}
	if nodeConfig.Global.Security.Enabled {
		nodeConfig.Global.Security.ClusterPrivateFile = clusterPrivateKeyFile
		nodeConfig.Global.Security.UserList = usersConfigFile
	}
	nodeConfig.Local.Network.ListenOn = fmt.Sprintf("127.0.0.1:%d", d.port)
	err = writeJsonToFile(d.config, nodeConfig)
	if err != nil {
		return err
	}
	return nil
}

func (d *db) setInfo(s Security) {
	d.config = "qdb"
	d.data = "db"
	d.port = qdbPort
	addBinarybase(&d.bin)
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
	addBinaryExtension(&d.bin)
	d.config += ".cfg"
}

func (d *db) start() error {
	d.exe = exec.Command(d.bin, "-c", d.config)
	d.exe.Start()
	return nil
}

func (d *db) stop() error {
	return d.exe.Process.Kill()
}

func (d *db) remove() error {
	var finalError error
	err := os.Remove(d.bin)
	if err != nil {
		fmt.Println("Could not remove", d.bin)
		finalError = err
	}
	err = os.Remove(d.config)
	if err != nil {
		fmt.Println("Could not remove", d.config)
		finalError = err
	}
	err = os.RemoveAll(d.data)
	if err != nil {
		fmt.Println("Could not remove", d.data)
		finalError = err
	}
	d.bin = ""
	d.data = ""
	d.config = ""
	return finalError
}
