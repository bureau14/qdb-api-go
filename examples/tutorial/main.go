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
		fmt.Printf("Failed to connect to QuasarDB: %s", err.Error())
		panic("Failed to connect to QuasarDB")
	}

	dropTable(handle)

	err = createTable(handle)
	if err != nil {
		fmt.Printf("Failed to create table: %s", err.Error())
		panic("Failed to create table")
	}

	err = batchInsert(handle)
	if err != nil {
		fmt.Printf("Failed to insert data: %s", err.Error())
		panic("Failed to insert data")
	}

	err = columnInsert(handle)
	if err != nil {
		fmt.Printf("Failed to column insert data: %s", err.Error())
		panic("Failed to column insert data")
	}

	err = bulkRead(handle)
	if err != nil {
		fmt.Printf("Failed to bulk read data: %s", err.Error())
		panic("Failed to bulk read data")
	}

	err = columnRead(handle)
	if err != nil {
		fmt.Printf("Failed to column read data: %s", err.Error())
		panic("Failed to column read data")
	}

	err = dropTable(handle)
	if err != nil {
		fmt.Printf("Failed to drop table: %s", err.Error())
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
	if err != nil {
		return err
	}
	// tags-start

	// Once you've created a table you can attach tags to it
	err = table.AttachTag("nasdaq")
	// tags-end
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

func columnInsert(handle *qdb.HandleType) error {
	// column-insert-start
	table := handle.Timeseries("stocks")

	openColumn := table.DoubleColumn("open")
	closeColumn := table.DoubleColumn("close")
	volumeColumn := table.Int64Column("volume")

	t1 := time.Unix(1600000000, 0)
	t2 := time.Unix(1610000000, 0)

	openPoints := []qdb.TsDoublePoint{
		qdb.NewTsDoublePoint(t1, 3.40),
		qdb.NewTsDoublePoint(t2, 3.40),
	}

	closePoints := []qdb.TsDoublePoint{
		qdb.NewTsDoublePoint(t1, 3.50),
		qdb.NewTsDoublePoint(t2, 3.55),
	}

	volumePoints := []qdb.TsInt64Point{
		qdb.NewTsInt64Point(t1, 10000),
		qdb.NewTsInt64Point(t2, 7500),
	}

	err := openColumn.Insert(openPoints...)
	if err != nil {
		return err
	}

	err = closeColumn.Insert(closePoints...)
	if err != nil {
		return err
	}

	err = volumeColumn.Insert(volumePoints...)
	if err != nil {
		return err
	}
	// column-insert-end

	return nil
}

func columnRead(handle *qdb.HandleType) error {
	// column-get-start
	table := handle.Timeseries("stocks")

	openColumn := table.DoubleColumn("open")
	closeColumn := table.DoubleColumn("close")
	volumeColumn := table.Int64Column("volume")

	timeRange := qdb.NewRange(time.Unix(1600000000, 0), time.Unix(1610000001, 0))

	openResults, err := openColumn.GetRanges(timeRange)
	if err != nil {
		return err
	}

	closeResults, err := closeColumn.GetRanges(timeRange)
	if err != nil {
		return err
	}

	volumeResults, err := volumeColumn.GetRanges(timeRange)
	if err != nil {
		return err
	}

	for i := 0; i < 2; i++ {
		timestamp := openResults[i].Timestamp()
		open := openResults[i].Content()
		close := closeResults[i].Content()
		volume := volumeResults[i].Content()

		fmt.Printf("timestamp: %s, open: %v, close: %v, volume: %v\n", timestamp, open, close, volume)
	}

	// column-get-end

	return nil
}

func dropTable(handle *qdb.HandleType) error {
	// drop-table-start
	table := handle.Timeseries("stocks")
	err := table.Remove()
	// drop-table-end
	return err
}
