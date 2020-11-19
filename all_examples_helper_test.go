package qdb

import (
	"fmt"
	"time"
)

func MustCreateTimeseries(alias string) (*HandleType, *TimeseriesEntry) {
	h := MustSetupHandle(insecureURI, 120*time.Second)
	timeseries := h.Timeseries(alias)
	return &h, &timeseries
}

func MustCreateTimeseriesWithColumns(alias string) (*HandleType, *TimeseriesEntry) {
	h, timeseries := MustCreateTimeseries(alias)
	timeseries.Create(24*time.Hour, NewTsColumnInfo("series_column_blob", TsColumnBlob), NewTsColumnInfo("series_column_double", TsColumnDouble), NewTsColumnInfo("series_column_int64", TsColumnInt64), NewTsColumnInfo("series_column_string", TsColumnString), NewTsColumnInfo("series_column_timestamp", TsColumnTimestamp))
	return h, timeseries
}

func MustCreateTimeseriesWithData(alias string) (*HandleType, *TimeseriesEntry) {
	h, timeseries := MustCreateTimeseriesWithColumns(alias)
	blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns, symbolColumns, err := timeseries.Columns()
	if err != nil {
		panic(err)
	}

	var count int64 = 4
	timestamps := make([]time.Time, count)
	blobPoints := make([]TsBlobPoint, count)
	doublePoints := make([]TsDoublePoint, count)
	int64Points := make([]TsInt64Point, count)
	stringPoints := make([]TsStringPoint, count)
	timestampPoints := make([]TsTimestampPoint, count)
	symbolPoints := make([]TsSymbolPoint, count)
	for idx := int64(0); idx < count; idx++ {
		timestamps[idx] = time.Unix((idx+1)*10, 0)
		blobPoints[idx] = NewTsBlobPoint(timestamps[idx], []byte(fmt.Sprintf("content_%d", idx)))
		doublePoints[idx] = NewTsDoublePoint(timestamps[idx], float64(idx))
		int64Points[idx] = NewTsInt64Point(timestamps[idx], idx)
		stringPoints[idx] = NewTsStringPoint(timestamps[idx], fmt.Sprintf("content_%d", idx))
		timestampPoints[idx] = NewTsTimestampPoint(timestamps[idx], timestamps[idx])
		symbolPoints[idx] = NewTsSymbolPoint(timestamps[idx], fmt.Sprintf("content_%d", idx))
	}
	err = blobColumns[0].Insert(blobPoints...)
	if err != nil {
		panic(err)
	}
	err = doubleColumns[0].Insert(doublePoints...)
	if err != nil {
		panic(err)
	}
	err = int64Columns[0].Insert(int64Points...)
	if err != nil {
		panic(err)
	}
	err = stringColumns[0].Insert(stringPoints...)
	if err != nil {
		panic(err)
	}
	err = timestampColumns[0].Insert(timestampPoints...)
	if err != nil {
		panic(err)
	}
	err = symbolColumns[0].Insert(symbolPoints...)
	if err != nil {
		panic(err)
	}
	return h, timeseries
}
