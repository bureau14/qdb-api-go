package qdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
)

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
	setBinaryRights(dst)
	return
}

func writeJsonToFile(path string, jsonObject interface{}) error {
	data, err := json.MarshalIndent(&jsonObject, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, data, 0744)
}

func checkInput() (string, string, string) {
	qdbBinariesPath := os.Getenv("QDB_SERVER_PATH")
	if qdbBinariesPath == "" {
		panic(errors.New("A path to qdb binaries shall be provided - set 'QDB_SERVER_PATH' environment varialbe"))
	}

	qdbd := mkBinaryPath(qdbBinariesPath, "qdbd")
	qdbUserAdd := mkBinaryPath(qdbBinariesPath, "qdb_user_add")
	qdbClusterKeygen := mkBinaryPath(qdbBinariesPath, "qdb_cluster_keygen")

	found := true
	if _, err := os.Stat(qdbd); os.IsNotExist(err) {
		fmt.Println("qdbd binary is needed to run the tests")
		found = false
	}
	if _, err := os.Stat(qdbUserAdd); os.IsNotExist(err) {
		fmt.Println("qdb_user_add binary is needed to run the tests")
		found = false
	}
	if _, err := os.Stat(qdbClusterKeygen); os.IsNotExist(err) {
		fmt.Println("qdb_cluster_keygen binary is needed to run the tests")
		found = false
	}
	if found == false {
		panic(errors.New("Binaries are missing"))
	}
	return qdbd, qdbUserAdd, qdbClusterKeygen
}

func generateUser(qdbUserAdd string) error {
	output, err := exec.Command(qdbUserAdd, "-u", "test-user", "-p", usersConfigFile, "-s", userPrivateKeyFile, "--uid=1", "--superuser=1").Output()
	if err != nil {
		return fmt.Errorf(string(output))
	}
	return nil
}

func generateClusterKeys(qdbClusterKeygen string) error {
	output, err := exec.Command(qdbClusterKeygen, "-p", clusterPublicKeyFile, "-s", clusterPrivateKeyFile).Output()
	if err != nil {
		return fmt.Errorf(string(output))
	}
	return nil
}

func cleanup() {
	os.Remove(clusterPrivateKeyFile)
	os.Remove(clusterPublicKeyFile)
	os.Remove(userPrivateKeyFile)
	os.Remove(usersConfigFile)
}
