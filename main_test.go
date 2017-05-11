package qdb

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func startQdbServer(qdbPath string) {
	runQdbServer := exec.Command(qdbPath)
	runQdbServer.Start()

	time.Sleep(5 * time.Second)
}

func checkQdbServerStarted() bool {
	var getQdbServerPidsCmd bytes.Buffer
	getQdbServerPidsCmd.WriteString("pgrep -f qdbd")
	getQdbServerPids := exec.Command(getQdbServerPidsCmd.String())
	var outbuf, errbuf bytes.Buffer
	getQdbServerPids.Stdout = &outbuf
	getQdbServerPids.Stderr = &errbuf
	getQdbServerPids.Start()
	getQdbServerPids.Wait()
	if outbuf.String() == "" {
		return false
	}
	return true
}

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags

	qdbPath := os.Getenv("QDB_SERVER_PATH")
	if qdbPath == "" {
		fmt.Printf("No path found for qdb server\n")
		os.Exit(-1)
	}
	fmt.Printf("Using qdb server: %s\n", qdbPath)
	if checkQdbServerStarted() == false {
		fmt.Printf("Qdb server was not running, starting now.\n")
		startQdbServer(qdbPath)
	}

	retCode := m.Run()

	os.Exit(retCode)
}
