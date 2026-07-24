package main

// import-start

import (
	"fmt"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
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

	err = bulkRead(handle)
	if err != nil {
		fmt.Printf("Failed to bulk read data: %s", err.Error())
		panic("Failed to bulk read data")
	}

	err = query(handle)
	if err != nil {
		fmt.Printf("Failed to query data: %s", err.Error())
		panic("Failed to query data")
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
	columns := []qdb.WriterColumn{
		{ColumnName: "open", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "close", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "volume", ColumnType: qdb.TsColumnInt64},
	}

	table, err := qdb.NewWriterTable("stocks", columns)
	if err != nil {
		return err
	}

	table.SetIndex([]time.Time{
		time.Unix(1548979200, 0),
		time.Unix(1549065600, 0),
	})

	open := qdb.NewColumnDataDouble([]float64{3.40, 3.50})
	close := qdb.NewColumnDataDouble([]float64{3.50, 3.55})
	volume := qdb.NewColumnDataInt64([]int64{10000, 7500})
	err = table.SetDatas([]qdb.ColumnData{&open, &close, &volume})
	if err != nil {
		return err
	}

	writer := qdb.NewWriterWithDefaultOptions()
	err = writer.SetTable(table)
	if err != nil {
		return err
	}

	err = writer.Push(*handle)
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

func query(handle *qdb.HandleType) error {
	// query-start

	query := handle.Query("SELECT SUM(volume) FROM stocks")
	table, err := query.Execute()
	if err != nil {
		return err
	}

	for _, row := range table.Rows() {
		for _, col := range table.Columns(row) {
			fmt.Printf("%v ", col.Get().Value())
		}
		fmt.Println()
	}

	// query-end
	return nil
}

func dropTable(handle *qdb.HandleType) error {
	// drop-table-start
	table := handle.Timeseries("stocks")
	err := table.Remove()
	// drop-table-end
	return err
}
