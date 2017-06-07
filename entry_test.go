package qdb

import (
	"bytes"
	"strconv"
	"testing"
	"time"
)

// test every entry related files
func TestEntry(t *testing.T) {
	handle, err := setupHandle()
	if err != nil {
		panic(err)
	}
	defer handle.Close()
	// Test blobs
	blobTest(t, handle)

	// Test integers
	integerTest(t, handle)

	// Test timeseries
	timeseriesTest(t, handle)

	// Test tags
	tagsTest(t, handle)

	// Test alias
	aliasTest(t, handle)

	// Test expiry
	expiryTest(t, handle)

	// Test location
	locationTest(t, handle)

	// Test metadata
	metadataTest(t, handle)
}

func blobTest(t *testing.T, handle HandleType) {
	blobEmptyAlias := handle.Blob("")
	content := []byte("content")
	err := blobEmptyAlias.Put(content, NeverExpires())
	if err == nil {
		t.Error("Should not be able to put with empty alias")
	}
	err = blobEmptyAlias.RemoveIf(content)
	if err == nil {
		t.Error("Should not be able to remove with empty alias")
	}

	aliasEmptyContent := generateAlias(16)
	blobEmptyContent := handle.Blob(aliasEmptyContent)
	err = blobEmptyContent.Put([]byte{}, NeverExpires())
	if err != nil {
		t.Error("Should be able to put empty content without error got: ", err)
	}
	contentObtained, err := blobEmptyContent.Get()
	if err != nil {
		t.Error("Should be able to get without error got: ", err)
	}
	if bytes.Equal(contentObtained, []byte{}) == false {
		t.Error("Updated content should be ", []byte{}, " got ", contentObtained)
	}

	contentObtained, err = blobEmptyContent.GetAndUpdate([]byte{}, NeverExpires())
	if err != nil {
		t.Error("Should be able to 'get and update' on empty content without error got: ", err)
	}
	if bytes.Equal(contentObtained, []byte{}) == false {
		t.Error("Data retrieved should be ", []byte{}, " got: ", contentObtained)
	}
	contentObtained, err = blobEmptyContent.CompareAndSwap(content, []byte{}, NeverExpires())
	if err != nil {
		t.Error("Should be able to 'compare and swap' on empty content without error got: ", err)
	}
	if bytes.Equal(contentObtained, []byte{}) == false {
		t.Error("Data retrieved should be ", []byte{}, " got: ", contentObtained)
	}
	contentObtained, err = blobEmptyContent.Get()
	if bytes.Equal(contentObtained, content) == false {
		t.Error("Data retrieved should be ", content, " got: ", contentObtained)
	}
	err = blobEmptyContent.Remove()
	if err != nil {
		t.Error("Should be able to remove without error got: ", err)
	}

	alias := generateAlias(16)
	blob := handle.Blob(alias)

	err = blob.Put(content, NeverExpires())
	if err != nil {
		t.Error("Should be able to put without error got: ", err)
	}
	err = blob.Put(content, NeverExpires())
	if err == nil {
		t.Error("You should not be able to put an alias a second time.")
	}
	contentObtained, err = blob.Get()
	if err != nil {
		t.Error("Should be able to get without error got: ", err)
	}
	if bytes.Equal(contentObtained, content) == false {
		t.Error("Data put should be ", content, " got: ", contentObtained)
	}

	newContent := []byte{}
	err = blob.Update(newContent, NeverExpires())
	if err != nil {
		t.Error("Should be able to update with an empty content without error got: ", err)
	}
	contentObtained, err = blob.Get()
	if err != nil {
		t.Error("Should be able to get an empty content without error got: ", err)
	}
	if bytes.Equal(contentObtained, newContent) == false {
		t.Error("Updated content should be ", newContent, " got ", contentObtained)
	}
	newContent = []byte("newContent")
	err = blob.Update(newContent, NeverExpires())
	if err != nil {
		t.Error("Should be able to update without error got: ", err)
	}
	contentObtained, err = blob.Get()
	if err != nil {
		t.Error("Should be able to get without error got: ", err)
	}
	if bytes.Equal(contentObtained, newContent) == false {
		t.Error("Updated content should be ", newContent, " got ", contentObtained)
	}

	err = blob.Remove()
	if err != nil {
		t.Error("Should be able to remove without error got: ", err)
	}
	contentObtained, err = blob.Get()
	if err == nil {
		t.Error("Should not be able to get without error got: nil")
	}
	if bytes.Equal(contentObtained, []byte{}) == false {
		t.Error("Expected contentObtained to be empty got: ", contentObtained)
	}

	err = blob.Put(content, NeverExpires())
	if err != nil {
		t.Error("Should be able to reuse removed alias without error got: ", err)
	}

	contentObtained, err = blob.GetAndUpdate(newContent, NeverExpires())
	if err != nil {
		t.Error("Should be able to 'get and update' without error got: ", err)
	}
	if bytes.Equal(contentObtained, content) == false {
		t.Error("Data retrieved should be ", content, " got: ", contentObtained)
	}

	badContent := []byte("badContent")
	contentObtained, err = blob.CompareAndSwap([]byte{}, badContent, NeverExpires())
	if err == nil {
		t.Error("Should not be able to 'compare and swap' with bad comparand.")
	}

	contentObtained, err = blob.CompareAndSwap(content, newContent, NeverExpires())
	if err != nil {
		t.Error("Should be able to 'compare and swap' without error got: ", err)
	}
	if bytes.Equal(contentObtained, []byte{}) == false {
		t.Error("Data retrieved should be ", []byte{}, " got: ", contentObtained)
	}

	contentObtained, err = blob.GetAndRemove()
	if err != nil {
		t.Error("Should be able to 'get and remove' without error got: ", err)
	}
	if bytes.Equal(contentObtained, content) == false {
		t.Error("Data retrieved should be ", content, " got: ", contentObtained)
	}
	contentObtained, err = blob.Get()
	if err == nil {
		t.Error("Should not be able to get without error got: nil")
	}

	err = blob.Put(content, NeverExpires())
	if err != nil {
		t.Error("Should be able to reuse removed alias without error got: ", err)
	}
	err = blob.RemoveIf([]byte{})
	if err == nil {
		t.Error("Should not be able to remove with empty content.")
	}
	err = blob.RemoveIf(content)
	if err != nil {
		t.Error("Should be able to remove with content: ", content, " got: ", err)
	}
	contentObtained, err = blob.Get()
	if err == nil {
		t.Error("Should not be able to get without error.")
	}
}

func integerTest(t *testing.T, handle HandleType) {
	alias := generateAlias(16)
	content := int64(13)
	integer := handle.Integer(alias)

	// Test IntegerPut
	err := integer.Put(content, NeverExpires())
	if err != nil {
		t.Error("You should be able to put without error - got: ", err)
	}
	err = integer.Put(content, NeverExpires())
	if err == nil {
		t.Error("You should not be able to put an alias a second time.")
	}
	contentObtained, err := integer.Get()
	if err != nil {
		t.Error("You should be able to get without error - got: ", err)
	}
	if contentObtained != content {
		t.Error("Data retrieved should be ", content, " got: ", contentObtained)
	}

	newContent := int64(87)
	err = integer.Update(newContent, NeverExpires())
	if err != nil {
		t.Error("You should be able to update without error - got: ", err)
	}
	contentObtained, err = integer.Get()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if contentObtained != newContent {
		t.Error("Data retrieved should be ", newContent, " got: ", contentObtained)
	}

	toAdd := int64(5)
	expected := toAdd + newContent
	result, err := integer.Add(toAdd)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if expected != result {
		t.Error("Result should be ", expected, " got: ", result)
	}

	err = integer.Remove()
	if err != nil {
		t.Error("You should be able to remove without error - got: ", err)
	}
	contentObtained, err = integer.Get()
	if err == nil {
		t.Error("You should not be able to get without error got: nil")
	}
	if contentObtained != 0 {
		t.Error("Content retrieved should be set to zero, got: ", contentObtained)
	}
}

func timeseriesTest(t *testing.T, handle HandleType) {
	timeseriesEmptyAlias := handle.Timeseries("", []TsColumnInfo{NewTsColumnInfo("serie_column_blob", TsColumnBlob)})
	err := timeseriesEmptyAlias.Create()
	if err == nil {
		t.Error("You should not be able to create with empty alias.")
	}
	timeseriesEmptyColumns := handle.Timeseries("", []TsColumnInfo{})
	err = timeseriesEmptyColumns.Create()
	if err == nil {
		t.Error("You should not be able to create with no columns.")
	}

	alias := generateAlias(16)
	timeseries := handle.Timeseries(alias, []TsColumnInfo{NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble)})

	err = timeseries.Create()
	if err != nil {
		t.Error("You should be able to create with no error - got: ", err)
	}
	err = timeseries.Create()
	if err == nil {
		t.Error("You should not be able to create the same timeseries a second time.")
	}

	// declare range now for later use
	tsRange := NewTsRange(TimespecType{0, 0}, TimespecType{40, 0})
	tsRanges := []TsRange{tsRange}

	// Testing double point
	contentDouble := float64(3.4)
	contentDouble2 := float64(4.4)
	doublePoint1 := NewTsDoublePoint(TimespecType{10, 0}, contentDouble)
	doublePoint2 := NewTsDoublePoint(TimespecType{20, 0}, contentDouble2)

	err = timeseries.InsertDouble("serie_column_double", []TsDoublePoint{doublePoint1, doublePoint2})
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	err = timeseries.InsertDouble("serie_column_double", []TsDoublePoint{})
	if err == nil {
		t.Error("You should not be able to insert no content.")
	}

	// Testing double ranges
	tsDoublePoints, err := timeseries.GetDoubleRanges("serie_column_double", []TsRange{})
	if err == nil {
		t.Error("You should not be able to get empty ranges.")
	}
	tsDoublePoints, err = timeseries.GetDoubleRanges("serie_column_double", tsRanges)
	if err != nil {
		t.Error("You should be able to get result for this range.")
	}
	if len(tsDoublePoints) != 2 {
		t.Error("Expected len(tsDoublePoints) == 2 got", len(tsDoublePoints))
	}
	if tsDoublePoints[0].Timestamp != doublePoint1.Timestamp {
		t.Error("Expected timestamp ", doublePoint1.Timestamp, " got ", tsDoublePoints[0].Timestamp)
	}
	if tsDoublePoints[0].Content != contentDouble {
		t.Error("Expected content ", contentDouble, " got ", tsDoublePoints[0].Content)
	}

	// Testing double aggregations
	emptyDoubleAggs := []TsDoubleAggregation{}
	err = timeseries.GetDoubleAggregate("serie_column_double", &emptyDoubleAggs)
	if err == nil {
		t.Error("You should not be able to get empty aggregations.")
	}
	doubleAgg := TsDoubleAggregation{T: AggFirst, R: tsRange}
	doubleAggs := []TsDoubleAggregation{doubleAgg}
	err = timeseries.GetDoubleAggregate("serie_column_double", &doubleAggs)
	if err != nil {
		t.Error("You should be able to get result for this range.")
	}
	if doublePoint1.Content != doubleAggs[0].P.Content {
		t.Error("You should have obtained ", doublePoint1.Content, " but got ", doubleAggs[0].P.Content)
	}

	// Testing blob point
	contentBlob := []byte("data")
	blobPoint1 := NewTsBlobPoint(TimespecType{10, 0}, contentBlob)
	blobPoint2 := NewTsBlobPoint(TimespecType{20, 0}, []byte{})

	err = timeseries.InsertBlob("serie_column_blob", []TsBlobPoint{blobPoint1, blobPoint2})
	if err != nil {
		t.Error(err.Error())
	}
	err = timeseries.InsertBlob("serie_column_blob", []TsBlobPoint{})
	if err == nil {
		t.Error("You should not be able to insert no content.")
	}

	// Testing blob ranges
	tsBlobPoints, err := timeseries.GetBlobRanges("serie_column_blob", []TsRange{})
	if err == nil {
		t.Error("You should not be able to get empty ranges.")
	}
	tsBlobPoints, err = timeseries.GetBlobRanges("serie_column_blob", tsRanges)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if len(tsBlobPoints) != 2 {
		t.Error("Expected len(tsBlobPoints) == 2 got", len(tsBlobPoints))
	}
	if tsBlobPoints[0].Timestamp.Equals(blobPoint1.Timestamp) == false {
		t.Error("Expected timestamp ", blobPoint1.Timestamp, " got ", tsBlobPoints[0].Timestamp)
	}
	if bytes.Equal(tsBlobPoints[0].Content, contentBlob) == false {
		t.Error("Expected content ", contentBlob, " got ", tsBlobPoints[0].Content)
	}
	if bytes.Equal(tsBlobPoints[1].Content, []byte{}) == false {
		t.Error("Expected content ", []byte{}, " got ", tsBlobPoints[1].Content)
	}

	// Testing blob aggregations
	emptyBlobAggs := []TsBlobAggregation{}
	err = timeseries.GetBlobAggregate("serie_column_blob", &emptyBlobAggs)
	if err == nil {
		t.Error("You should not be able to get empty aggregations.")
	}
	blobAgg := TsBlobAggregation{T: AggFirst, R: tsRange}
	blobAggs := []TsBlobAggregation{blobAgg}
	err = timeseries.GetBlobAggregate("serie_column_blob", &blobAggs)
	if err != nil {
		t.Error("You should be able to get result for this range.")
	}
	if bytes.Equal(blobPoint1.Content, blobAggs[0].P.Content) == false {
		t.Error("You should have obtained ", blobPoint1.Content, " but got ", blobAggs[0].P.Content)
	}
}

func tagsTest(t *testing.T, handle HandleType) {
	// Create dumb integer to test tags
	alias := generateAlias(16)
	content := int64(13)
	integer := handle.Integer(alias)
	integer.Put(content, NeverExpires())

	// Test Attach tag
	err := integer.AttachTag("")
	if err == nil {
		t.Error("You should not be able to attach empty tag.")
	}
	tag := generateAlias(5)
	err = integer.AttachTag(tag)
	if err != nil {
		t.Error("You should be able to attach normal tag.")
	}

	// Test Has tag
	err = integer.HasTag(tag)
	if err != nil {
		t.Error("You should be able to check normal tag.")
	}
	err = integer.HasTag("")
	if err == nil {
		t.Error("You should not be able to check empty tag.")
	}

	// Test Get tagged
	aliases, err := integer.GetTagged(tag)
	if err != nil {
		t.Error("You should be able to get values tagged.")
	}
	if len(aliases) != 1 {
		t.Error("Expected len(aliases) to be one got: ", len(aliases))
	}
	if aliases[0] != alias {
		t.Error("Alias should be ", alias, " got ", aliases[0])
	}
	aliases, err = integer.GetTagged("")
	if err == nil {
		t.Error("You should not be able to retrieve empty tag.")
	}

	// Test Get tags
	var tags []string
	tags, err = integer.GetTags()
	if err != nil {
		t.Error("You should be able to get tags.")
	}
	if len(tags) != 1 {
		t.Error("Expected len(tags) to be one got: ", len(tags))
	}
	if tags[0] != tag {
		t.Error("Tag is ", tags[0], " should be ", tag)
	}

	// Test Detach tag
	err = integer.DetachTag(tag)
	if err != nil {
		t.Error("You should be able to detach tag.")
	}
	err = integer.HasTag(tag)
	if err == nil {
		t.Error("Expected an error - got nil")
	}
	// Test GetTags on nil
	tags, err = integer.GetTags()
	if err != nil {
		t.Error("You should be able to get tags even if there are none.")
	}
	if tags == nil || len(tags) != 0 {
		t.Error("Expected tags to be an empty array")
	}

	// Attach multiple tags
	err = integer.AttachTags([]string{})
	if err == nil {
		t.Error("You should not be able to attach empty tags.")
	}
	tag0 := "tag0"
	tag1 := "tag1"
	err = integer.AttachTags([]string{tag0, tag1})
	tags, err = integer.GetTags()
	if err != nil {
		t.Error("You should be able to get multiple tags.")
	}
	if len(tags) != 2 {
		t.Error("Expected len(tags) to be 2 got: ", len(tags))
	}
	if tags[0] != tag0 || tags[1] != tag1 {
		t.Error("Tags should be ", tag0, " - ", tag1, " got: ", tags[0], " - ", tags[1])
	}

	// Detach multiple tags
	err = integer.DetachTags([]string{})
	if err == nil {
		t.Error("You should not be able to detach empty tags.")
	}
	err = integer.DetachTags([]string{tag0, tag1})
	tags, err = integer.GetTags()
	if err != nil {
		t.Error("You should be able to get tag even if there are none.")
	}
	if tags == nil || len(tags) != 0 {
		t.Error("Expected tags to be an empty array")
	}

	// Test Remove
	err = integer.Remove()
	if err != nil {
		t.Error("You should be able to remove an alias.")
	}
	_, err = integer.Get()
	if err == nil {
		t.Error("You should not be able to get an empty alias.")
	}

	tags, err = integer.GetTags()
	if err == nil {
		t.Error("You should not be able to get tags on an empty alias.")
	}
}

func aliasTest(t *testing.T, handle HandleType) {
	alias := generateAlias(16)
	integer := handle.Integer(alias)

	if integer.Alias() != alias {
		t.Error("Alias should be ", alias, " got ", integer.Alias())
	}
}

func expiryTest(t *testing.T, handle HandleType) {
	alias := generateAlias(16)
	integer := handle.Integer(alias)

	expiry := time.Unix(int64(time.Now().Second())-1, 0)
	err := integer.ExpiresFromNow(expiry)
	if err == nil {
		t.Error("Should not be able to set expiry time without alias")
	}

	integer.Put(3, NeverExpires())

	expiry = time.Unix(13, 0)
	err = integer.ExpiresAt(expiry)
	if err == nil {
		t.Error("Should not be able to set expiry time in the past")
	}
	expiry = time.Now()
	err = integer.ExpiresAt(expiry)
	if err != nil {
		t.Error("Should be able to set expiry time in the future: ", err)
	}

	expiry = time.Unix(2, 0)
	err = integer.ExpiresFromNow(expiry)
	if err != nil {
		t.Error("Should be able to set expiry time in the future: ", err)
	}
}

func locationTest(t *testing.T, handle HandleType) {
	alias := generateAlias(16)
	integer := handle.Integer(alias)

	integer.Put(3, NeverExpires())
	location, err := integer.GetLocation()
	if err != nil {
		t.Error("Should be able to get locationof content: ", err)
	}
	port, _ := strconv.Atoi(getenv("QDB_PORT", "2836"))
	if location.Port != int16(port) {
		t.Error("location port should be ", port, " got ", location.Port)
	}
}

func metadataTest(t *testing.T, handle HandleType) {
	alias := generateAlias(16)
	integer := handle.Integer(alias)

	metadata, err := integer.GetMetadata()
	if err == nil {
		t.Error("Should not be able to get metadata without alias")
	}

	integer.Put(3, NeverExpires())
	metadata, err = integer.GetMetadata()
	if err != nil {
		t.Error("Should be able to get metadata: ", err)
	}
	if metadata.Type != EntryInteger {
		t.Error("Entry type should be ", EntryInteger, " got ", metadata.Type)
	}
}
