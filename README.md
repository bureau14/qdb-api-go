Quasardb Go API
=================
[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/bureau14/qdb-api-go)

**Note:** The Go API works on Windows x64 and Unix-like operating systems. 32-bit Windows is not currently supported.

**Warning:** Early stage development version. Use at your own risk.

**Warning:** Go1.9.0 is not supported. If you encounter a compilation problem with time.go please upgrade.

Go API for [quasardb](https://www.quasardb.net/).


### Requirements

1. [Go compiler and tools](https://golang.org/)
1. [quasardb daemon](https://www.quasardb.net/download/index.html)
1. [quasardb C API](https://www.quasardb.net/download/index.html) version corresponding to the OS you use
1. The version of the quasardb C API *must match* the current git branch, the master branch corresponds to the nightly C API

### Build instructions:
1. `go get -d github.com/bureau14/qdb-api-go`
1. Extract the downloaded C API into `$GOPATH/src/github.com/bureau14/qdb-api-go/thirdparty/quasardb`

### Test instructions:
1.  `export QDB_SERVER_PATH=/path/to/qdb-server/bin` # a path to a dir containing qdbd, qdb_cluster_keygen and qdb_user_add executables
1. `cd $GOPATH/src/github.com/bureau14/qdb-api-go`
1. `go test`

### Coverage instructions:
1. `export QDB_SERVER_PATH=/path/to/qdb-server/bin` # a path to a dir containing qdbd, qdb_cluster_keygen and qdb_user_add executables
1. `cd $GOPATH/src/github.com/bureau14/qdb-api-go`
1. `go test -coverprofile=coverage.out`
1. `go tool cover -html=coverage.out` # if you want to see coverage detail in a browser

### Usage (OS X)
1. `mkdir $GOPATH/src/<PROJECT>`
1. Extract the downloaded C API into `$GOPATH/src/<PROJECT>/thirdparty/quasardb`
1. Ensure quasardb C library is in the **`DYLD_LIBRARY_PATH`**: `export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$GOPATH/src/<PROJECT>/thirdparty/quasardb/lib`

### Troubleshooting

If you encounter any problems, please create an issue in the bug tracker.

### Getting started
## Simple test
Assuming a non secured database (see "Setup a secured connection" section for secured databases)
```
import qdb "github.com/bureau14/qdb-api-go"
import "time"

func main() {
    handle, err := qdb.SetupHandle("qdb://127.0.0.1:2836", time.Duration(120) * time.Second)
    if err != nil {
        // do something with error
    }
    defer handle.Close()

    blob := handle.Blob("alias")

    content := []byte("content")
    err = blob.Put(content, qdb.NeverExpires())
    if err != nil {
        // do something with error
    }

    contentUpdate := []byte("updated content")
    err = blob.Update(contentUpdate, qdb.NeverExpires())
    if err != nil {
        // do something with error
    }

    blob.Remove()
}
```

The following tests samples are presuming you import as specified in the previous example.
The error checking will be ommited for brevity.

## Setup a non secure connection
```
    handle, err := qdb.SetupHandle("qdb://127.0.0.1:2836", time.Duration(120) * time.Second)

    // alternatively:
    handle := qdb.MustSetupHandle("qdb://127.0.0.1:2836", time.Duration(120) * time.Second)
```

## Setup a secured connection
```
    handle, err := qdb.SetupSecureHandle("qdb://127.0.0.1:2836", "/path/to/cluster_public.key", "/path/to/user_private.key", time.Duration(120) * time.Second, qdb.EncryptNone)

    // alternatively:
    handle := qdb.MustSetupSecureHandle("qdb://127.0.0.1:2836", "/path/to/cluster_public.key", "/path/to/user_private.key", time.Duration(120) * time.Second, qdb.EncryptNone)
```

## Setup a handle manually
This could prove useful if you need to manage the flow of creation of your handle.
```
    handle, err := qdb.NewHandle()

    // Set timeout
    err = handle.SetTimeout(time.Duration(120) * time.Second)

    // Set encryption if enabled server side
    err = handle.SetEncryption(qdb.EncryptAES)

    // add security if enabled server side
    clusterKey, err := ClusterKeyFromFile("/path/to/cluster_public.key")
    err = handle.AddClusterPublicKey(clusterKey)
    user, secret, err := ClusterKeyFromFile("/path/to/cluster_public.key")
    err = handle.AddUserCredentials(user, secret)

    // connect
    err = handle.Connect("qdb://127.0.0.1:2836)
```
