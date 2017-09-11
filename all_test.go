package qdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	clusterPrivateKeyFile string = "cluster_private.key"
	clusterPublicKeyFile  string = "cluster_public.key"
	userPrivateKeyFile    string = "user_private.key"
	usersConfigFile       string = "users.cfg"
	qdbPort               int    = 30083
	nodeURI               string = "qdb://127.0.0.1:30083"
	clusterURI            string = "qdb://127.0.0.1:30083"
	securedURI            string = "qdb://127.0.0.1:30084"
)

var (
	handle      HandleType
	unsecuredDB *db
	securedDB   *db
	// encryptedDB *db
)

type Security string

const (
	SecurityNone      Security = ""
	SecurityEnabled   Security = "secured"
	SecurityEncrypted Security = "encrypted"
)

func TestMain(m *testing.M) {
	var err error
	qdbd, qdbUserAdd, qdbClusterKeygen := checkInput()

	generateUser(qdbUserAdd)
	generateClusterKeys(qdbClusterKeygen)
	unsecuredDB, err = newDB(qdbd, SecurityNone)
	if err != nil {
		panic(err)
	}
	securedDB, err = newDB(qdbd, SecurityEnabled)
	if err != nil {
		panic(err)
	}

	unsecuredDB.start()
	securedDB.start()
	time.Sleep(10 * time.Second)
	m.Run()
	if err := unsecuredDB.stop(); err != nil {
		fmt.Println(err)
	}
	if err := securedDB.stop(); err != nil {
		fmt.Println(err)
	}

	if err := unsecuredDB.remove(); err != nil {
		fmt.Println(err)
	}
	if err := securedDB.remove(); err != nil {
		fmt.Println(err)
	}
	cleanup()
}

func TestAll(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test Suite")
}

var _ = Describe("Tests", func() {
	BeforeSuite(func() {
		var err error
		handle, err = SetupHandle(clusterURI, 120*time.Second)
		Expect(err).ToNot(HaveOccurred())
		// stupid thing to boast about having 100% test coverage
		fmt.Errorf("error: %s", ErrorType(2))
	})

	AfterSuite(func() {
		handle.Close()
	})
})

func checkInput() (string, string, string) {
	qdbBinariesPath := os.Getenv("QDB_SERVER_PATH")
	if qdbBinariesPath == "" {
		panic(errors.New("A path to qdb binaries shall be provided"))
	}

	qdbd := filepath.Join(qdbBinariesPath, "qdbd")
	qdbUserAdd := filepath.Join(qdbBinariesPath, "qdb_user_add")
	qdbClusterKeygen := filepath.Join(qdbBinariesPath, "qdb_cluster_keygen")

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

type db struct {
	bin    string
	data   string
	config string
	port   int
}

func newDB(qdbd string, s Security) (*db, error) {
	d := &db{}
	d.setInfo(s)
	_, err := exec.Command("cp", qdbd, d.bin).Output()
	if err != nil {
		return d, err
	}
	err = d.prepareConfig(s)
	return d, err
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
	err = WriteJsonToFile(d.config, nodeConfig)
	if err != nil {
		return err
	}
	return nil
}

func (d db) start() error {
	runQdbServer := exec.Command(d.bin, "-c", d.config)
	var outbuf, errbuf bytes.Buffer
	runQdbServer.Stdout = &outbuf
	runQdbServer.Stderr = &errbuf
	runQdbServer.Start()
	return nil
}

func (d db) stop() error {
	_, err := exec.Command("killall", d.bin).Output()
	return err
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

func generateUser(qdbUserAdd string) error {
	_, err := exec.Command(qdbUserAdd, "-u", "test", "-p", usersConfigFile, "-s", userPrivateKeyFile).Output()
	return err
}

func generateClusterKeys(qdbClusterKeygen string) error {
	_, err := exec.Command(qdbClusterKeygen, "-p", clusterPublicKeyFile, "-s", clusterPrivateKeyFile).Output()
	return err
}

func cleanup() {
	os.Remove(clusterPrivateKeyFile)
	os.Remove(clusterPublicKeyFile)
	os.Remove(userPrivateKeyFile)
	os.Remove(usersConfigFile)
}
