package qdbtests

import (
	"bytes"
	"testing"

	. "github.com/bureau14/qdb-api-go"
)

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
	if tsDoublePoints[0].Timestamp != doublePoint1.Timestamp {
		t.Error("Expected timestamp ", doublePoint1.Timestamp, " got ", tsDoublePoints[0].Timestamp)
	}
	if tsDoublePoints[0].Content != contentDouble {
		t.Error("Expected content ", contentDouble, " got ", tsDoublePoints[0].Content)
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
	if tsBlobPoints[0].Timestamp != blobPoint1.Timestamp {
		t.Error("Expected timestamp ", blobPoint1.Timestamp, " got ", tsBlobPoints[0].Timestamp)
	}
	if bytes.Equal(tsBlobPoints[0].Content, contentBlob) == false {
		t.Error("Expected content ", contentBlob, " got ", tsBlobPoints[0].Content)
	}
}
