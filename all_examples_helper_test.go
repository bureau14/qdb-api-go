package qdb

import (
	"fmt"
	"time"
)

func createTimeseries(alias string) (*HandleType, *TimeseriesEntry) {
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return nil, nil
	}
	timeseries := h.Timeseries(alias)
	return &h, &timeseries
}

func createTimeseriesWithColumns(alias string) (*HandleType, *TimeseriesEntry) {
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return nil, nil
	}
	timeseries := h.Timeseries(alias)
	timeseries.Create(24*time.Hour, NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble))
	return &h, &timeseries
}

func createTimeseriesWithData(alias string) (*HandleType, *TimeseriesEntry) {
	h, err := SetupHandle(clusterURI, 120*time.Second)
	if err != nil {
		return nil, nil
	}
	timeseries := h.Timeseries(alias)
	timeseries.Create(24*time.Hour, NewTsColumnInfo("serie_column_blob", TsColumnBlob), NewTsColumnInfo("serie_column_double", TsColumnDouble))
	doubleColumns, blobColumns, err := timeseries.Columns()

	var count int64 = 4
	timestamps := make([]time.Time, count)
	blobPoints := make([]TsBlobPoint, count)
	doublePoints := make([]TsDoublePoint, count)
	for idx := int64(0); idx < count; idx++ {
		timestamps[idx] = time.Unix((idx+1)*10, 0)
		blobPoints[idx] = NewTsBlobPoint(timestamps[idx], []byte(fmt.Sprintf("content_%d", idx)))
		doublePoints[idx] = NewTsDoublePoint(timestamps[idx], float64(idx))
	}
	doubleColumns[0].Insert(doublePoints...)
	blobColumns[0].Insert(blobPoints...)
	return &h, &timeseries
}
