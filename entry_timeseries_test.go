package qdb

import "testing"
import "bytes"

func TestTimeseries(t *testing.T) {
	handle, err := setupHandle()
	if err != nil {
		t.Error("Setup failed: ", err)
	}

	alias := generateAlias(16)
	contentBlob := []byte("data")
	contentDouble := float64(3.4)
	blobPoint1 := NewTsBlobPoint(TimespecType{10, 0}, contentBlob)
	blobPoint2 := NewTsBlobPoint(TimespecType{20, 0}, contentBlob)
	doublePoint1 := NewTsDoublePoint(TimespecType{10, 0}, contentDouble)
	doublePoint2 := NewTsDoublePoint(TimespecType{20, 0}, contentDouble)
	timeseries := handle.Timeserie(alias, []TsColumnInfo{NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble)})

	err = timeseries.Create()
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}

	tsRange := NewTsRange(TimespecType{0, 0}, TimespecType{40, 0})
	tsRanges := []TsRange{tsRange}

	// Testing double point
	err = timeseries.InsertDouble("serie_column_double", []TsDoublePoint{doublePoint1, doublePoint2})
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	var tsDoublePoints []TsDoublePoint
	tsDoublePoints, err = timeseries.GetDoubleRanges("serie_column_double", tsRanges)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if len(tsDoublePoints) != 2 {
		t.Error("Expected len(tsDoublePoints) == 2 got", len(tsDoublePoints))
	}
	if tsDoublePoints[0].timestamp != doublePoint1.timestamp {
		t.Error("Expected timestamp ", doublePoint1.timestamp, " got ", tsDoublePoints[0].timestamp)
	}
	if tsDoublePoints[0].content != contentDouble {
		t.Error("Expected content ", contentDouble, " got ", tsDoublePoints[0].content)
	}

	// Testing blob point
	err = timeseries.InsertBlob("serie_column_blob", []TsBlobPoint{blobPoint1, blobPoint2})
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	var tsBlobPoints []TsBlobPoint
	tsBlobPoints, err = timeseries.GetBlobRanges("serie_column_blob", tsRanges)
	if err != nil {
		t.Error("Expected no error - got: ", err)
	}
	if len(tsBlobPoints) != 2 {
		t.Error("Expected len(tsBlobPoints) == 2 got", len(tsBlobPoints))
	}
	if tsBlobPoints[0].timestamp != blobPoint1.timestamp {
		t.Error("Expected timestamp ", blobPoint1.timestamp, " got ", tsBlobPoints[0].timestamp)
	}
	if bytes.Equal(tsBlobPoints[0].content, contentBlob) == false {
		t.Error("Expected content ", contentBlob, " got ", tsBlobPoints[0].content)
	}
}
