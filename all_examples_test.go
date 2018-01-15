package qdb

import (
	"fmt"
	"time"
)

func ExampleHandleType() {
	var h HandleType
	h.Open(ProtocolTCP)
	fmt.Printf("API build: %s\n", h.APIVersion())
	// Output: API build: 2.3.0master
}

func ExampleEntry_Alias() {
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return
	}
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
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return
	}
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
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return
	}
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
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return
	}
	defer h.Close()

	timeseries := h.Timeseries("TimeseriesAlias")

	err = timeseries.Create(24*time.Hour, NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble))
	if err != nil {
		return
	}
	defer timeseries.Remove()

	blobColumn := timeseries.BlobColumn("serie_column_blob")
	doubleColumn := timeseries.DoubleColumn("serie_column_double")

	tsRange := NewRange(time.Unix(0, 0), time.Unix(40, 0))
	tsRanges := append([]TsRange{}, tsRange)

	contentDouble := float64(3.4)
	doublePoint1 := NewTsDoublePoint(time.Unix(10, 0), contentDouble)
	doublePoint2 := NewTsDoublePoint(time.Unix(20, 0), contentDouble)
	doublePoints := append([]TsDoublePoint{}, doublePoint1, doublePoint2)
	doubleColumn.Insert(doublePoints...)

	tsDoublePoints, _ := doubleColumn.GetRanges(tsRanges...)

	fmt.Println("Timestamp first double value:", tsDoublePoints[0].Timestamp().UTC())
	fmt.Println("Content first double value:", tsDoublePoints[0].Content())

	blobPoint1 := NewTsBlobPoint(time.Unix(10, 0), []byte("data"))
	blobPoint2 := NewTsBlobPoint(time.Unix(20, 0), []byte("data"))
	blobPoints := append([]TsBlobPoint{}, blobPoint1, blobPoint2)
	blobColumn.Insert(blobPoints...)

	tsBlobPoints, _ := blobColumn.GetRanges(tsRanges...)

	fmt.Println("Timestamp second blob value:", tsBlobPoints[1].Timestamp().UTC())
	fmt.Println("Content second blob value:", string(tsBlobPoints[1].Content()))

	// Output:
	// Timestamp first double value: 1970-01-01 00:00:10 +0000 UTC
	// Content first double value: 3.4
	// Timestamp second blob value: 1970-01-01 00:00:20 +0000 UTC
	// Content second blob value: data
}

func ExampleNode() {
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return
	}
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
	// Status - Max sessions: 20000
	// Config - Root Depot: db
	// Config - Listen On: 127.0.0.1:30083
	// Topology - Successor is same as predecessor: true
}

func ExampleQuery() {
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return
	}
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

	_, err = h.Query().NotTag("second").Execute()
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
