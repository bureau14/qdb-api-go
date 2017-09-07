package qdb

import (
	"fmt"
	"time"
)

func ExampleHandleType() {
	var handle HandleType
	handle.Open(ProtocolTCP)
	fmt.Printf("API build: %s\n", handle.APIVersion())
	// Output: API build: 2.1.0master
}

func ExampleEntry_Alias() {
	handle, err := SetupHandle("qdb://127.0.0.1:30083", 120*time.Second)
	if err != nil {
		return
	}
	defer handle.Close()

	blob1 := handle.Blob("BLOB_1")
	blob1.Put([]byte("blob 1 content"), NeverExpires())
	defer blob1.Remove()
	blob2 := handle.Blob("BLOB_2")
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
	handle, err := SetupHandle("qdb://127.0.0.1:30083", 120*time.Second)
	if err != nil {
		return
	}
	defer handle.Close()

	alias := "BlobAlias"
	blob := handle.Blob(alias)
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
	handle, err := SetupHandle("qdb://127.0.0.1:30083", 120*time.Second)
	if err != nil {
		return
	}
	defer handle.Close()

	alias := "IntAlias"
	integer := handle.Integer(alias)

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
	handle, err := SetupHandle("qdb://127.0.0.1:30083", 120*time.Second)
	if err != nil {
		return
	}
	defer handle.Close()

	alias := "TimeseriesAlias"
	columns := []TsColumnInfo{NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble)}
	timeseries := handle.Timeseries(alias, columns)
	defer timeseries.Remove()

	timeseries.Create()

	tsRange := TsRange{time.Unix(0, 0), time.Unix(40, 0)}
	tsRanges := []TsRange{tsRange}

	contentDouble := float64(3.4)
	doublePoint1 := NewTsDoublePoint(time.Unix(10, 0), contentDouble)
	doublePoint2 := NewTsDoublePoint(time.Unix(20, 0), contentDouble)
	timeseries.InsertDouble("serie_column_double", []TsDoublePoint{doublePoint1, doublePoint2})

	var tsDoublePoints []TsDoublePoint
	tsDoublePoints, _ = timeseries.GetDoubleRanges("serie_column_double", tsRanges)

	fmt.Println("Timestamp first double value:", tsDoublePoints[0].Timestamp)
	fmt.Println("Content first double value:", tsDoublePoints[0].Content)

	contentBlob := []byte("data")
	blobPoint1 := NewTsBlobPoint(time.Unix(10, 0), contentBlob)
	blobPoint2 := NewTsBlobPoint(time.Unix(20, 0), contentBlob)
	timeseries.InsertBlob("serie_column_blob", []TsBlobPoint{blobPoint1, blobPoint2})

	var tsBlobPoints []TsBlobPoint
	tsBlobPoints, _ = timeseries.GetBlobRanges("serie_column_blob", tsRanges)

	fmt.Println("Timestamp second blob value:", tsBlobPoints[1].Timestamp)
	fmt.Println("Content second blob value:", string(tsBlobPoints[1].Content))

	// Output:
	// Timestamp first double value: 1970-01-01 01:00:10 +0100 CET
	// Content first double value: 3.4
	// Timestamp second blob value: 1970-01-01 01:00:20 +0100 CET
	// Content second blob value: data
}

func ExampleNode() {
	handle, err := SetupHandle("qdb://127.0.0.1:30083", 120*time.Second)
	if err != nil {
		return
	}
	defer handle.Close()

	status, _ := handle.Node("qdb://127.0.0.1:30083").Status()
	fmt.Println("Max sessions:", status.Network.Partitions.MaxSessions)
	// Output:
	// Max sessions: 20000
}
