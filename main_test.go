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

func startQdbServer(qdbPath string) {
	random := rand.Intn(1000)
	port := 30000 + random
	fmt.Printf("Opening qdbd on port %d\n", port)
	var reg bytes.Buffer
	reg.WriteString(qdbPath)
	reg.WriteString(" -a 127.0.0.1:")
	reg.WriteString(strconv.Itoa(port))
	runQdbServer := exec.Command(qdbPath)
	runQdbServer.Start()

	time.Sleep(5 * time.Second)
}

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags

	qdbPath := os.Getenv("QDB_SERVER_PATH")
	if qdbPath == "" {
		fmt.Printf("No path found for qdb server\n")
		os.Exit(-1)
	}
	fmt.Printf("Using qdb server: %s\n", qdbPath)
	startQdbServer(qdbPath)

	retCode := m.Run()

	os.Exit(retCode)
}
