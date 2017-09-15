package qdb

import (
	"fmt"
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
	time.Sleep(10 * time.Second)

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
		Expect(string(fmt.Errorf("error: %s", ErrorType(2)).Error())).To(Equal("error: An unknown error occurred."))
	})

	AfterSuite(func() {
		handle.Close()
	})
})
