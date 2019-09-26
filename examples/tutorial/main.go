package main

// import-start

import (
	"fmt"
	"time"

	qdb "github.com/bureau14/qdb-api-go"
)

// import-end

func main() {

	handle, err := connect()
	if err != nil {
		fmt.Printf("Failed to connect to QuasarDB")
		panic("Failed to connect to QuasarDB")
	}

	err = createTable(handle)
	if err != nil {
		fmt.Printf("Failed to create table")
		panic("Failed to create table")
	}

	err = batchInsert(handle)
	if err != nil {
		fmt.Printf("Failed to insert data")
		panic("Failed to insert data")
	}

	err = bulkRead(handle)
	if err != nil {
		fmt.Printf("Failed to buld read data: %s", err.Error())
		panic("Failed to buld read data")
	}

	err = dropTable(handle)
	if err != nil {
		fmt.Printf("Failed to drop table")
		panic("Failed to drop table")
	}
}

func connect() (*qdb.HandleType, error) {
	// connect-start
	clusterURI := "qdb://127.0.0.1:2836"
	timeoutDuration := time.Duration(120) * time.Second
	handle, err := qdb.SetupHandle(clusterURI, timeoutDuration)
	// connect-end

	if err != nil {
		return nil, err
	}

	return &handle, nil
}

func secureConnect() (*qdb.HandleType, error) {
	// secure-connect-start
	clusterURI := "qdb://127.0.0.1:2836"
	timeoutDuration := time.Duration(120) * time.Second
	clusterPublicKeyPath := "/path/to/cluster_public.key"
	usersPrivateKeyPath := "/path/to/user_private.key"
	handle, err := qdb.SetupSecuredHandle(
		clusterURI,
		clusterPublicKeyPath,
		usersPrivateKeyPath,
		timeoutDuration,
		qdb.EncryptNone,
	)
	// secure-connect-end

	if err != nil {
		return nil, err
	}

	return &handle, nil
}

func createTable(handle *qdb.HandleType) error {
	// create-table-start
	table := handle.Timeseries("stocks")
	columnsInfo := []qdb.TsColumnInfo{
		qdb.NewTsColumnInfo("open", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("close", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("volume", qdb.TsColumnInt64),
	}
	shardSizeDuration := 24 * time.Hour

	err := table.Create(shardSizeDuration, columnsInfo...)
	// create-table-end
	return err
}

func batchInsert(handle *qdb.HandleType) error {
	// batch-insert-start
	batchColumnInfo := []qdb.TsBatchColumnInfo{
		qdb.NewTsBatchColumnInfo("stocks", "open", 2),
		qdb.NewTsBatchColumnInfo("stocks", "close", 2),
		qdb.NewTsBatchColumnInfo("stocks", "volume", 2),
	}

	batch, err := handle.TsBatch(batchColumnInfo...)
	if err != nil {
		return err
	}

	batch.StartRow(time.Unix(1548979200, 0))
	batch.RowSetDouble(0, 3.40)
	batch.RowSetDouble(1, 3.50)
	batch.RowSetInt64(2, 10000)

	batch.StartRow(time.Unix(1549065600, 0))
	batch.RowSetDouble(0, 3.50)
	batch.RowSetDouble(1, 3.55)
	batch.RowSetInt64(2, 7500)

	err = batch.Push()
	// batch-insert-end

	return err
}

func bulkRead(handle *qdb.HandleType) error {
	// bulk-read-start
	table := handle.Timeseries("stocks")
	bulk, err := table.Bulk()
	if err != nil {
		return err
	}

	err = bulk.GetRanges(qdb.NewRange(time.Unix(1548979200, 0), time.Unix(1549065601, 0)))
	if err != nil {
		return err
	}

	for {
		timestamp, err := bulk.NextRow()
		if err != nil {
			break
		}

		open, err := bulk.GetDouble()
		if err != nil {
			return err
		}
		close, err := bulk.GetDouble()
		if err != nil {
			return err
		}
		volume, err := bulk.GetInt64()
		if err != nil {
			return err
		}

		fmt.Printf("timestamp: %s, open: %v, close: %v, volume: %v\n", timestamp, open, close, volume)
	}
	// bulk-read-end
	return nil
}

func dropTable(handle *qdb.HandleType) error {
	// drop-table-start
	table := handle.Timeseries("stocks")
	err := table.Remove()
	// drop-table-end
	return err
}
