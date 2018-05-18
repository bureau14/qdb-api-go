package qdb

import (
	"fmt"
	"time"
)

func ExampleHandleType() {
	var h HandleType
	h.Open(ProtocolTCP)
	fmt.Printf("API build: %s\n", h.APIVersion())
	// Output: API build: 2.6.0master
}

func ExampleEntry_Alias() {
	h := MustSetupHandle(clusterURI, 120*time.Second)
	defer h.Close()

	blob1 := h.Blob("BLOB_1")
	blob1.Put([]byte("blob 1 content"), NeverExpires())
	defer blob1.Remove()
	blob2 := h.Blob("BLOB_2")
	blob2.Put([]byte("blob 2 content"), NeverExpires())
	defer blob2.Remove()

	fmt.Println("Alias blob 1:", blob1.Alias())
	fmt.Println("Alias blob 2:", blob2.Alias())

	tags1 := []string{"tag blob 1", "tag both blob"}
	blob1.AttachTags(tags1)
	defer blob1.DetachTags(tags1)
	tags2 := []string{"tag blob 2", "tag both blob"}
	blob2.AttachTags(tags2)
	defer blob2.DetachTags(tags2)

	resultTagBlob1, _ := blob1.GetTagged("tag blob 1")
	fmt.Println("Tagged with 'tag blob 1':", resultTagBlob1)
	resultTagBlob2, _ := blob1.GetTagged("tag blob 2")
	fmt.Println("Tagged with 'tag blob 2':", resultTagBlob2)
	resultTagBoth, _ := blob1.GetTagged("tag both blob")
	fmt.Println("Tagged with 'tag both blob':", resultTagBoth)

	// Output: Alias blob 1: BLOB_1
	// Alias blob 2: BLOB_2
	// Tagged with 'tag blob 1': [BLOB_1]
	// Tagged with 'tag blob 2': [BLOB_2]
	// Tagged with 'tag both blob': [BLOB_2 BLOB_1]

}

func ExampleBlobEntry() {
	h := MustSetupHandle(clusterURI, 120*time.Second)
	defer h.Close()

	alias := "BlobAlias"
	blob := h.Blob(alias)
	defer blob.Remove()

	content := []byte("content")
	blob.Put(content, NeverExpires())

	obtainedContent, _ := blob.Get()
	fmt.Println("Get content:", string(obtainedContent))

	updateContent := []byte("updated content")
	blob.Update(updateContent, PreserveExpiration())

	obtainedContent, _ = blob.Get()
	fmt.Println("Get updated content:", string(obtainedContent))

	newContent := []byte("new content")
	previousContent, _ := blob.GetAndUpdate(newContent, PreserveExpiration())
	fmt.Println("Previous content:", string(previousContent))

	obtainedContent, _ = blob.Get()
	fmt.Println("Get new content:", string(obtainedContent))

	// Output:
	// Get content: content
	// Get updated content: updated content
	// Previous content: updated content
	// Get new content: new content
}

func ExampleIntegerEntry() {
	h := MustSetupHandle(clusterURI, 120*time.Second)
	defer h.Close()

	alias := "IntAlias"
	integer := h.Integer(alias)

	integer.Put(int64(3), NeverExpires())
	defer integer.Remove()

	obtainedContent, _ := integer.Get()
	fmt.Println("Get content:", obtainedContent)

	newContent := int64(87)
	integer.Update(newContent, NeverExpires())

	obtainedContent, _ = integer.Get()
	fmt.Println("Get updated content:", obtainedContent)

	integer.Add(3)

	obtainedContent, _ = integer.Get()
	fmt.Println("Get added content:", obtainedContent)

	// Output:
	// Get content: 3
	// Get updated content: 87
	// Get added content: 90
}

func ExampleTimeseriesEntry() {
	h := MustSetupHandle(clusterURI, 120*time.Second)
	defer h.Close()
	timeseries := h.Timeseries("alias")

	fmt.Println("timeseries:", timeseries.Alias())
	// Output:
	// timeseries: alias
}

func ExampleTimeseriesEntry_Create() {
	h, timeseries := MustCreateTimeseries("ExampleTimeseriesEntry_Create")
	defer h.Close()

	// duration, columns...
	timeseries.Create(24*time.Hour, NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble))
}

func ExampleTimeseriesEntry_Columns() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTimeseriesEntry_Columns")
	defer h.Close()

	doubleColumns, blobColumns, int64Columns, timestampColumns, err := timeseries.Columns()
	if err != nil {
		// handle error
	}
	for _, col := range doubleColumns {
		fmt.Println("column:", col.Name())
		// do something like Insert, GetRanges with a double column
	}
	for _, col := range blobColumns {
		fmt.Println("column:", col.Name())
		// do something like Insert, GetRanges with a blob column
	}
	for _, col := range int64Columns {
		fmt.Println("column:", col.Name())
		// do something like Insert, GetRanges with a blob column
	}
	for _, col := range timestampColumns {
		fmt.Println("column:", col.Name())
		// do something like Insert, GetRanges with a blob column
	}
	// Output:
	// column: serie_column_double
	// column: serie_column_blob
	// column: serie_column_int64
	// column: serie_column_timestamp
}

func ExampleTimeseriesEntry_ColumnsInfo() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTimeseriesEntry_ColumnsInfo")
	defer h.Close()

	columns, err := timeseries.ColumnsInfo()
	if err != nil {
		// handle error
	}
	for _, col := range columns {
		fmt.Println("column:", col.Name())
	}
	// Output:
	// column: serie_column_blob
	// column: serie_column_double
	// column: serie_column_int64
	// column: serie_column_timestamp
}

func ExampleTimeseriesEntry_InsertColumns() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTimeseriesEntry_InsertColumns")
	defer h.Close()

	err := timeseries.InsertColumns(NewTsColumnInfo("serie_column_blob_2", TsColumnBlob), NewTsColumnInfo("serie_column_double_2", TsColumnDouble))
	if err != nil {
		// handle error
	}
	columns, err := timeseries.ColumnsInfo()
	if err != nil {
		// handle error
	}
	for _, col := range columns {
		fmt.Println("column:", col.Name())
	}
	// Output:
	// column: serie_column_blob
	// column: serie_column_double
	// column: serie_column_int64
	// column: serie_column_timestamp
	// column: serie_column_blob_2
	// column: serie_column_double_2
}

func ExampleTimeseriesEntry_DoubleColumn() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTimeseriesEntry_DoubleColumn")
	defer h.Close()

	column := timeseries.DoubleColumn("serie_column_double")
	fmt.Println("column:", column.Name())
	// Output:
	// column: serie_column_double
}

func ExampleTsDoubleColumn_Insert() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTsDoubleColumn_Insert")
	defer h.Close()

	column := timeseries.DoubleColumn("serie_column_double")

	// Insert only one point:
	column.Insert(NewTsDoublePoint(time.Now(), 3.2))

	// Insert multiple points
	doublePoints := make([]TsDoublePoint, 2)
	doublePoints[0] = NewTsDoublePoint(time.Now(), 3.2)
	doublePoints[1] = NewTsDoublePoint(time.Now(), 4.8)

	err := column.Insert(doublePoints...)
	if err != nil {
		// handle error
	}
}

func ExampleTsDoubleColumn_GetRanges() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsDoubleColumn_GetRanges")
	defer h.Close()

	column := timeseries.DoubleColumn("serie_column_double")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	doublePoints, err := column.GetRanges(r)
	if err != nil {
		// handle error
	}
	for _, point := range doublePoints {
		fmt.Println("timestamp:", point.Timestamp(), "- value:", point.Content())
	}
	// Output:
	// timestamp: 1970-01-01 01:00:10 +0100 CET - value: 0
	// timestamp: 1970-01-01 01:00:20 +0100 CET - value: 1
	// timestamp: 1970-01-01 01:00:30 +0100 CET - value: 2
	// timestamp: 1970-01-01 01:00:40 +0100 CET - value: 3
}

func ExampleTsDoubleColumn_EraseRanges() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsDoubleColumn_EraseRanges")
	defer h.Close()

	column := timeseries.DoubleColumn("serie_column_double")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	numberOfErasedValues, err := column.EraseRanges(r)
	if err != nil {
		// handle error
	}
	fmt.Println("Number of erased values:", numberOfErasedValues)
	// Output:
	// Number of erased values: 4
}

func ExampleTsDoubleColumn_Aggregate() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsDoubleColumn_Aggregate")
	defer h.Close()

	column := timeseries.DoubleColumn("serie_column_double")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	aggFirst := NewDoubleAggregation(AggFirst, r)
	aggMean := NewDoubleAggregation(AggArithmeticMean, r)
	results, err := column.Aggregate(aggFirst, aggMean)
	if err != nil {
		// handle error
	}
	fmt.Println("first:", results[0].Result().Content())
	fmt.Println("mean:", results[1].Result().Content())
	fmt.Println("number of elements reviewed for mean:", results[1].Count())
	// Output:
	// first: 0
	// mean: 1.5
	// number of elements reviewed for mean: 4
}

func ExampleTimeseriesEntry_BlobColumn() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTimeseriesEntry_BlobColumn")
	defer h.Close()

	column := timeseries.BlobColumn("serie_column_blob")
	fmt.Println("column:", column.Name())
	// Output:
	// column: serie_column_blob
}

func ExampleTsBlobColumn_Insert() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTsBlobColumn_Insert")
	defer h.Close()

	column := timeseries.BlobColumn("serie_column_blob")

	// Insert only one point:
	column.Insert(NewTsBlobPoint(time.Now(), []byte("content")))

	// Insert multiple points
	blobPoints := make([]TsBlobPoint, 2)
	blobPoints[0] = NewTsBlobPoint(time.Now(), []byte("content"))
	blobPoints[1] = NewTsBlobPoint(time.Now(), []byte("content_2"))

	err := column.Insert(blobPoints...)
	if err != nil {
		// handle error
	}
}

func ExampleTsBlobColumn_GetRanges() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsBlobColumn_GetRanges")
	defer h.Close()

	column := timeseries.BlobColumn("serie_column_blob")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	blobPoints, err := column.GetRanges(r)
	if err != nil {
		// handle error
	}
	for _, point := range blobPoints {
		fmt.Println("timestamp:", point.Timestamp(), "- value:", string(point.Content()))
	}
	// Output:
	// timestamp: 1970-01-01 01:00:10 +0100 CET - value: content_0
	// timestamp: 1970-01-01 01:00:20 +0100 CET - value: content_1
	// timestamp: 1970-01-01 01:00:30 +0100 CET - value: content_2
	// timestamp: 1970-01-01 01:00:40 +0100 CET - value: content_3
}

func ExampleTsBlobColumn_EraseRanges() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsBlobColumn_EraseRanges")
	defer h.Close()

	column := timeseries.BlobColumn("serie_column_blob")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	numberOfErasedValues, err := column.EraseRanges(r)
	if err != nil {
		// handle error
	}
	fmt.Println("Number of erased values:", numberOfErasedValues)
	// Output:
	// Number of erased values: 4
}

func ExampleTsBlobColumn_Aggregate() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsBlobColumn_Aggregate")
	defer h.Close()

	column := timeseries.BlobColumn("serie_column_blob")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	aggFirst := NewBlobAggregation(AggFirst, r)
	results, err := column.Aggregate(aggFirst)
	if err != nil {
		// handle error
	}
	fmt.Println("first:", string(results[0].Result().Content()))
	// Output:
	// first: content_0
}

func ExampleTimeseriesEntry_Int64Column() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTimeseriesEntry_Int64Column")
	defer h.Close()

	column := timeseries.Int64Column("serie_column_int64")
	fmt.Println("column:", column.Name())
	// Output:
	// column: serie_column_int64
}

func ExampleTsInt64Column_Insert() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTsInt64Column_Insert")
	defer h.Close()

	column := timeseries.Int64Column("serie_column_int64")

	// Insert only one point:
	column.Insert(NewTsInt64Point(time.Now(), 3))

	// Insert multiple points
	int64Points := make([]TsInt64Point, 2)
	int64Points[0] = NewTsInt64Point(time.Now(), 3)
	int64Points[1] = NewTsInt64Point(time.Now(), 4)

	err := column.Insert(int64Points...)
	if err != nil {
		// handle error
	}
}

func ExampleTsInt64Column_GetRanges() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsInt64Column_GetRanges")
	defer h.Close()

	column := timeseries.Int64Column("serie_column_int64")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	int64Points, err := column.GetRanges(r)
	if err != nil {
		// handle error
	}
	for _, point := range int64Points {
		fmt.Println("timestamp:", point.Timestamp(), "- value:", point.Content())
	}
	// Output:
	// timestamp: 1970-01-01 01:00:10 +0100 CET - value: 0
	// timestamp: 1970-01-01 01:00:20 +0100 CET - value: 1
	// timestamp: 1970-01-01 01:00:30 +0100 CET - value: 2
	// timestamp: 1970-01-01 01:00:40 +0100 CET - value: 3
}

func ExampleTsInt64Column_EraseRanges() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsInt64Column_EraseRanges")
	defer h.Close()

	column := timeseries.Int64Column("serie_column_int64")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	numberOfErasedValues, err := column.EraseRanges(r)
	if err != nil {
		// handle error
	}
	fmt.Println("Number of erased values:", numberOfErasedValues)
	// Output:
	// Number of erased values: 4
}

func ExampleTimeseriesEntry_TimestampColumn() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTimeseriesEntry_TimestampColumn")
	defer h.Close()

	column := timeseries.TimestampColumn("serie_column_timestamp")
	fmt.Println("column:", column.Name())
	// Output:
	// column: serie_column_timestamp
}

func ExampleTsTimestampColumn_Insert() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTsTimestampColumn_Insert")
	defer h.Close()

	column := timeseries.TimestampColumn("serie_column_timestamp")

	// Insert only one point:
	column.Insert(NewTsTimestampPoint(time.Now(), time.Now()))

	// Insert multiple points
	timestampPoints := make([]TsTimestampPoint, 2)
	timestampPoints[0] = NewTsTimestampPoint(time.Now(), time.Now())
	timestampPoints[1] = NewTsTimestampPoint(time.Now(), time.Now())

	err := column.Insert(timestampPoints...)
	if err != nil {
		// handle error
	}
}

func ExampleTsTimestampColumn_GetRanges() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsTimestampColumn_GetRanges")
	defer h.Close()

	column := timeseries.TimestampColumn("serie_column_timestamp")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	timestampPoints, err := column.GetRanges(r)
	if err != nil {
		// handle error
	}
	for _, point := range timestampPoints {
		fmt.Println("timestamp:", point.Timestamp(), "- value:", point.Content())
	}
	// Output:
	// timestamp: 1970-01-01 01:00:10 +0100 CET - value: 1970-01-01 01:00:10 +0100 CET
	// timestamp: 1970-01-01 01:00:20 +0100 CET - value: 1970-01-01 01:00:20 +0100 CET
	// timestamp: 1970-01-01 01:00:30 +0100 CET - value: 1970-01-01 01:00:30 +0100 CET
	// timestamp: 1970-01-01 01:00:40 +0100 CET - value: 1970-01-01 01:00:40 +0100 CET
}

func ExampleTsTimestampColumn_EraseRanges() {
	h, timeseries := MustCreateTimeseriesWithData("ExampleTsTimestampColumn_EraseRanges")
	defer h.Close()

	column := timeseries.TimestampColumn("serie_column_timestamp")

	r := NewRange(time.Unix(0, 0), time.Unix(40, 5))
	numberOfErasedValues, err := column.EraseRanges(r)
	if err != nil {
		// handle error
	}
	fmt.Println("Number of erased values:", numberOfErasedValues)
	// Output:
	// Number of erased values: 4
}

func ExampleTimeseriesEntry_Bulk() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTimeseriesEntry_Bulk")
	defer h.Close()

	bulk, err := timeseries.Bulk(NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble))
	if err != nil {
		// handle error
	}
	fmt.Println("RowCount:", bulk.RowCount())
	// Output:
	// RowCount: 0
}

func ExampleTsBulk_Push() {
	h, timeseries := MustCreateTimeseriesWithColumns("ExampleTsBulk_Push")
	defer h.Close()

	bulk, err := timeseries.Bulk(NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble))
	bulk.Row(time.Now()).Blob([]byte("content")).Double(3.2).Append()
	bulk.Row(time.Now()).Blob([]byte("content 2")).Double(4.8).Append()
	rowCount, err := bulk.Push()
	if err != nil {
		// handle error
		panic(err)
	}
	fmt.Println("RowCount:", rowCount)
	// Output:
	// RowCount: 2
}

func ExampleNode() {
	h := MustSetupHandle(clusterURI, 120*time.Second)
	defer h.Close()

	node := h.Node(nodeURI)

	status, _ := node.Status()
	fmt.Println("Status - Max sessions:", status.Network.Partitions.MaxSessions)

	config, _ := node.Config()
	fmt.Println("Config - Root Depot:", config.Local.Depot.Root)
	fmt.Println("Config - Listen On:", config.Local.Network.ListenOn)

	topology, _ := node.Topology()
	fmt.Println("Topology - Successor is same as predecessor:", topology.Successor.Endpoint == topology.Predecessor.Endpoint)
	// Output:
	// Status - Max sessions: 5000
	// Config - Root Depot: db
	// Config - Listen On: 127.0.0.1:30083
	// Topology - Successor is same as predecessor: true
}

func ExampleQuery() {
	h := MustSetupHandle(clusterURI, 120*time.Second)
	defer h.Close()

	var aliases []string
	aliases = append(aliases, generateAlias(16))
	aliases = append(aliases, generateAlias(16))

	blob := h.Blob("alias_blob")
	blob.Put([]byte("asd"), NeverExpires())
	defer blob.Remove()
	blob.AttachTag("all")
	blob.AttachTag("first")

	integer := h.Integer("alias_integer")
	integer.Put(32, NeverExpires())
	defer integer.Remove()
	integer.AttachTag("all")
	integer.AttachTag("second")

	var obtainedAliases []string
	obtainedAliases, _ = h.Query().Tag("all").Execute()
	fmt.Println("Get all aliases:", obtainedAliases)

	obtainedAliases, _ = h.Query().Tag("all").NotTag("second").Execute()
	fmt.Println("Get only first alias:", obtainedAliases)

	obtainedAliases, _ = h.Query().Tag("all").Type("int").Execute()
	fmt.Println("Get only integer alias:", obtainedAliases)

	obtainedAliases, _ = h.Query().Tag("adsda").Execute()
	fmt.Println("Get no aliases:", obtainedAliases)

	_, err := h.Query().NotTag("second").Execute()
	fmt.Println("Error:", err)

	_, err = h.Query().Type("int").Execute()
	fmt.Println("Error:", err)
	// Output:
	// Get all aliases: [alias_blob alias_integer]
	// Get only first alias: [alias_blob]
	// Get only integer alias: [alias_integer]
	// Get no aliases: []
	// Error: query should have at least one valid tag
	// Error: query should have at least one valid tag
}
