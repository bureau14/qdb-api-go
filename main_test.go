package qdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func createLocalQdbExe(qdbPath string) string {
	localQdbName := string("test_qdbd")
	runQdbServer := exec.Command("cp", qdbPath, localQdbName)
	runQdbServer.Start()
	runQdbServer.Wait()
	return localQdbName
}

func removeLocalDatabase(qdbPath string) {
	removeExe := exec.Command("rm", "-Rf", qdbPath)
	removeExe.Start()
	removeExe.Wait()
	removeDB := exec.Command("rm", "-Rf", "db/")
	removeDB.Start()
	removeDB.Wait()
}

func stopQdbServer(qdbPath string) {
	stopQdb := exec.Command("killall", qdbPath)
	stopQdb.Start()
	stopQdb.Wait()
}

func startQdbServer(qdbPath string) string {
	random := rand.Intn(1000)
	port := 30000 + random
	portStr := strconv.Itoa(port)
	exe := "./"
	exe += qdbPath
	fmt.Printf("Opening %s on port %s\n", qdbPath, portStr)
	address := "127.0.0.1:"
	address += portStr

	runQdbServer := exec.Command(exe, "-a", address)
	var outbuf, errbuf bytes.Buffer
	runQdbServer.Stdout = &outbuf
	runQdbServer.Stderr = &errbuf
	runQdbServer.Start()

	time.Sleep(5 * time.Second)
	return portStr
}

func setupHandle() (HandleType, error) {
	handle, err := NewHandle()
	qdbConnection := string("qdb://127.0.0.1:")
	qdbConnection += os.Args[1]
	err = handle.Connect(qdbConnection)
	return handle, err
}

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags

	qdbPath := os.Getenv("QDB_SERVER_PATH")
	if qdbPath == "" {
		fmt.Printf("No path found for qdb server\n")
		os.Exit(-1)
	}
	fmt.Printf("Using qdb server: %s\n", qdbPath)
	qdbPath = createLocalQdbExe(qdbPath)
	port := startQdbServer(qdbPath)

	oldArgs := os.Args

	os.Args = []string{qdbPath, port}

	retCode := m.Run()

	os.Args = oldArgs
	stopQdbServer(qdbPath)
	removeLocalDatabase(qdbPath)

	os.Exit(retCode)
}
