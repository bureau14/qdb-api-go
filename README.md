Quasardb Go API
=================

**Warning:** Early stage development version. Use at your own risk.

Go API for [quasardb](https://www.quasardb.net/).

See documentation at [doc.quasardb.net](https://doc.quasardb.net/2.0.0/api/java.html).

### Requirements

1. [quasardb daemon](https://download.quasardb.net/quasardb/)
1. [Go compiler and tools](https://golang.org/)

### Build instructions:
<<<<<<< 855421e89ff8c46693248265dc4a2c85c10a5b97
<<<<<<< 3500035c376f38cf4e5bb2865c506b2fed15169c
1. go get github.com/bureau14/qdb-api-go
=======
>>>>>>> Revert "Initial push"

### Test instructions:
1. export QDB_SERVER_PATH=/path/to/qdbd
2. cd $GOPATH/src/github.com/bureau14/qdb-api-go/tests
3. go test

### Examples instructions:
1. export QDB_SERVER_PATH=/path/to/qdbd
2. cd $GOPATH/src/github.com/bureau14/qdb-api-go/examples
3. go test
=======
1. git clone -b develop --single-branch git@github.com:vianneyPL/qdb-api-go.git $GOLANG/src/qdb
2. cd $GOLANG/src/qdb && go install && cd -

### Examples build instructions:
1. git clone -b examples --single-branch git@github.com:vianneyPL/qdb-api-go.git [directory]
2. cd [directory] && go build example[nb].go && ./example[nb]

TODO
>>>>>>> Update README.md

### Troubleshooting

If you encounter any problems, please create an issue in the bug tracker.
