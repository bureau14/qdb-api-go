Quasardb Go API
=================
[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/bureau14/qdb-api-go)

**Warning:** Early stage development version. Use at your own risk.

Go API for [quasardb](https://www.quasardb.net/).


### Requirements

1. [quasardb daemon](https://download.quasardb.net/quasardb/)
1. [Go compiler and tools](https://golang.org/)

### Build instructions:
1. go get github.com/bureau14/qdb-api-go

### Test instructions:
1. export QDB_SERVER_PATH=/path/to/qdbd # a path to a working qdbd executable
2. cd $GOPATH/src/github.com/bureau14/qdb-api-go
3. go test

### Coverage instructions:
1. export QDB_SERVER_PATH=/path/to/qdbd # a path to a working qdbd executable
2. cd $GOPATH/src/github.com/bureau14/qdb-api-go
3. go test -coverprofile=coverage.out
4. go tool cover -html=coverage.out # if you want to see coverage detail in a browser


### Troubleshooting

If you encounter any problems, please create an issue in the bug tracker.
