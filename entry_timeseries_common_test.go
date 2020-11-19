package qdb

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		alias string
	)

	BeforeEach(func() {
		alias = generateAlias(16)
	})

	// :: Timeseries tests ::
	// TODO(vianney): Debug timestamps, seems like they don't get to see the database
	Context("Timeseries - Common", func() {
		var (
			timeseries      TimeseriesEntry
			blobColumn      TsBlobColumn
			doubleColumn    TsDoubleColumn
			int64Column     TsInt64Column
			stringColumn    TsStringColumn
			timestampColumn TsTimestampColumn
			symbolColumn    TsSymbolColumn
			columnsInfo     []TsColumnInfo
		)
		const (
			count	int64 = 8
			start	int64 = 0
			end		int64 = count - 1

			blobIndex		int64 = 0
			doubleIndex		int64 = 1
			int64Index		int64 = 2
			stringIndex		int64 = 3
			timestampIndex	int64 = 4
			symbolIndex		int64 = 5

			blobColName      string = "blob_col"
			stringColName    string = "string_col"
			doubleColName    string = "double_col"
			int64ColName     string = "int64_col"
			timestampColName string = "timestamp_col"
			symbolColName    string = "symbol_col"

			symtableName     string = "my_symtable"
		)
		BeforeEach(func() {
			columnsInfo = []TsColumnInfo{}
			columnsInfo = append(columnsInfo, NewTsColumnInfo(blobColName, TsColumnBlob), NewTsColumnInfo(doubleColName, TsColumnDouble), NewTsColumnInfo(int64ColName, TsColumnInt64), NewTsColumnInfo(stringColName, TsColumnString), NewTsColumnInfo(timestampColName, TsColumnTimestamp), TsColumnInfo{symbolColName, TsColumnSymbol, symtableName})
		})
		JustBeforeEach(func() {
			timeseries = handle.Timeseries(alias)
		})
		AfterEach(func() {
			timeseries.Remove()
		})
		Context("Empty columns info", func() {
			BeforeEach(func() {
				columnsInfo = []TsColumnInfo{}
			})
			It("should create even with empty columns", func() {
				err := timeseries.Create(24*time.Hour, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should not work to get columns before creating the time series", func() {
				_, _, _, _, _, _, err := timeseries.Columns()
				Expect(err).To(HaveOccurred())
			})
			It("should work with zero columns", func() {
				err := timeseries.Create(24*time.Hour, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
				blobCols, doubleCols, int64Cols, stringCols, timestampCols, symbolCols, err := timeseries.Columns()
				Expect(err).ToNot(HaveOccurred())
				Expect(0).To(Equal(len(blobCols)))
				Expect(0).To(Equal(len(doubleCols)))
				Expect(0).To(Equal(len(int64Cols)))
				Expect(0).To(Equal(len(stringCols)))
				Expect(0).To(Equal(len(timestampCols)))
				Expect(0).To(Equal(len(symbolCols)))
			})
			It("should work with a shard size of 1ms", func() {
				err := timeseries.Create(1*time.Millisecond, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should not work with a shard size inferior to 1ms", func() {
				err := timeseries.Create(100*time.Nanosecond, columnsInfo...)
				Expect(err).To(HaveOccurred())
			})
		})
		Context("Created", func() {
			var (
				timestamps      []time.Time
				blobPoints      []TsBlobPoint
				doublePoints    []TsDoublePoint
				int64Points     []TsInt64Point
				stringPoints    []TsStringPoint
				timestampPoints []TsTimestampPoint
				symbolPoints    []TsSymbolPoint
			)
			BeforeEach(func() {
				timestamps = make([]time.Time, count)
				blobPoints = make([]TsBlobPoint, count)
				doublePoints = make([]TsDoublePoint, count)
				int64Points = make([]TsInt64Point, count)
				stringPoints = make([]TsStringPoint, count)
				timestampPoints = make([]TsTimestampPoint, count)
				symbolPoints = make([]TsSymbolPoint, count)
				for idx := int64(0); idx < count; idx++ {
					timestamps[idx] = time.Unix((idx+1)*10, 0)
					blobPoints[idx] = NewTsBlobPoint(timestamps[idx], []byte(fmt.Sprintf("content_%d", idx)))
					doublePoints[idx] = NewTsDoublePoint(timestamps[idx], float64(idx))
					int64Points[idx] = NewTsInt64Point(timestamps[idx], idx)
					stringPoints[idx] = NewTsStringPoint(timestamps[idx], fmt.Sprintf("content_%d", idx))
					timestampPoints[idx] = NewTsTimestampPoint(timestamps[idx], timestamps[idx])
					symbolPoints[idx] = NewTsSymbolPoint(timestamps[idx], fmt.Sprintf("content_%d", idx))
				}
			})
			JustBeforeEach(func() {
				err := timeseries.Create(24*time.Hour, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
				blobColumn = timeseries.BlobColumn(columnsInfo[blobIndex].Name())
				doubleColumn = timeseries.DoubleColumn(columnsInfo[doubleIndex].Name())
				int64Column = timeseries.Int64Column(columnsInfo[int64Index].Name())
				stringColumn = timeseries.StringColumn(columnsInfo[stringIndex].Name())
				timestampColumn = timeseries.TimestampColumn(columnsInfo[timestampIndex].Name())
				symbolColumn = timeseries.SymbolColumn(columnsInfo[symbolIndex].Name())
			})
			It("should have one column of each type", func() {
				blobCols, doubleCols, int64Cols, stringCols, timestampCols, err := timeseries.Columns()
				Expect(err).ToNot(HaveOccurred())
				Expect(1).To(Equal(len(doubleCols)))
				Expect(1).To(Equal(len(blobCols)))
				Expect(1).To(Equal(len(int64Cols)))
				Expect(1).To(Equal(len(stringCols)))
				Expect(1).To(Equal(len(timestampCols)))
				Expect(1).To(Equal(len(symbolCols)))
				Expect(TsColumnDouble).To(Equal(doubleCols[0].Type()))
				Expect(TsColumnBlob).To(Equal(blobCols[0].Type()))
				Expect(TsColumnInt64).To(Equal(int64Cols[0].Type()))
				Expect(TsColumnString).To(Equal(stringCols[0].Type()))
				Expect(TsColumnTimestamp).To(Equal(timestampCols[0].Type()))
				Expect(TsColumnSymbol).To(Equal(symbolCols[0].Type()))
			})
			Context("Insert Columns", func() {
				It("should work to insert new columns", func() {
					newColumns := []TsColumnInfo{NewTsColumnInfo(fmt.Sprintf("new_%s", blobColName), TsColumnBlob), NewTsColumnInfo(fmt.Sprintf("new_%s", doubleColName), TsColumnDouble), NewTsColumnInfo(fmt.Sprintf("new_%s", int64ColName), TsColumnInt64), NewTsColumnInfo(fmt.Sprintf("new_%s", stringColName), TsColumnString), NewTsColumnInfo(fmt.Sprintf("new_%s", timestampColName), TsColumnTimestamp), NewTsColumnInfo(fmt.Sprintf("new_%s", symbolColName), TsColumnSymbol, symtableName)}
					allColumns := append(columnsInfo, newColumns...)

					err := timeseries.InsertColumns(newColumns...)
					Expect(err).ToNot(HaveOccurred())

					cols, err := timeseries.ColumnsInfo()
					Expect(err).ToNot(HaveOccurred())
					Expect(cols).To(ConsistOf(allColumns))
				})
				It("should not work to insert no columns", func() {
					err := timeseries.InsertColumns()
					Expect(err).To(HaveOccurred())
				})
				It("should not work to insert columns with already existing names", func() {
					err := timeseries.InsertColumns(columnsInfo...)
					Expect(err).To(HaveOccurred())
				})
			})
			Context("Ranges", func() {
				It("should create a range", func() {
					r := NewRange(timestamps[start], timestamps[end])
					Expect(timestamps[start]).To(Equal(r.Begin()))
					Expect(timestamps[end]).To(Equal(r.End()))
				})
			})
			Context("Bulk", func() {
				Context("Insert", func() {
					var (
						blobValue      []byte    = []byte("content")
						doubleValue    float64   = 3.2
						int64Value     int64     = 2
						stringValue    string    = "content"
						timestampValue time.Time = time.Now()
						symbolValue    string    = "symbol"
					)
					It("should append all columns", func() {
						bulk, err := timeseries.Bulk()
						Expect(err).ToNot(HaveOccurred())
						for i := int64(0); i < count; i++ {
							err := bulk.Row(time.Now()).Blob(blobValue).Double(doubleValue).Int64(int64Value).String(stringValue).Timestamp(timestampValue).Symbol(symbolValue).Append()
							Expect(err).ToNot(HaveOccurred())
						}
						_, err = bulk.Push()
						Expect(err).ToNot(HaveOccurred())
						bulk.Release()
					})
					It("should append columns", func() {
						bulk, err := timeseries.Bulk(columnsInfo...)
						Expect(err).ToNot(HaveOccurred())
						for i := int64(0); i < count; i++ {
							err := bulk.Row(time.Now()).Blob(blobValue).Double(doubleValue).Int64(int64Value).String(stringValue).Timestamp(timestampValue).Symbol(symbolValue).Append()
							Expect(err).ToNot(HaveOccurred())
						}
						_, err = bulk.Push()
						Expect(err).ToNot(HaveOccurred())
						bulk.Release()
					})
					It("should append columns and ignore fields", func() {
						bulk, err := timeseries.Bulk(columnsInfo...)
						Expect(err).ToNot(HaveOccurred())
						for i := int64(0); i < count; i++ {
							err := bulk.Row(time.Now()).Ignore().Double(doubleValue).Int64(int64Value).String(stringValue).Timestamp(timestampValue).Append()
							Expect(err).ToNot(HaveOccurred())
						}
						_, err = bulk.Push()
						Expect(err).ToNot(HaveOccurred())
						bulk.Release()
					})
					It("should append columns on part of timeseries", func() {
						bulk, err := timeseries.Bulk(columnsInfo[0])
						Expect(err).ToNot(HaveOccurred())
						for i := int64(0); i < count; i++ {
							err := bulk.Row(time.Now()).Blob(blobValue).Append()
							Expect(err).ToNot(HaveOccurred())
						}
						Expect(count).To(Equal(int64(bulk.RowCount())))
						_, err = bulk.Push()
						Expect(err).ToNot(HaveOccurred())
						bulk.Release()
					})
					It("should fail to append columns - too much values", func() {
						bulk, err := timeseries.Bulk(columnsInfo...)
						Expect(err).ToNot(HaveOccurred())
						err = bulk.Row(time.Now()).Blob(blobValue).Double(doubleValue).Double(doubleValue).Append()
						Expect(err).To(HaveOccurred())
						bulk.Release()
					})
					It("should fail to append columns - additional column does not exist", func() {
						columnsInfo = append(columnsInfo, NewTsColumnInfo("asd", TsColumnDouble))
						_, err := timeseries.Bulk(columnsInfo...)
						Expect(err).To(HaveOccurred())
					})
					It("should fail to retrieve all columns from a non existing timeseries", func() {
						ts := handle.Timeseries("asd")
						_, err := ts.Bulk()
						Expect(err).To(HaveOccurred())
					})
				})

				Context("Get", func() {
					var (
						r TsRange
					)
					BeforeEach(func() {
						r = NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					})
					JustBeforeEach(func() {
						blobColumn.Insert(blobPoints...)
						doubleColumn.Insert(doublePoints...)
						int64Column.Insert(int64Points...)
						stringColumn.Insert(stringPoints...)
						timestampColumn.Insert(timestampPoints...)
						symbolColumn.Insert(symbolPoints...)
					})
					It("Should work to get all values", func() {
						bulk, err := timeseries.Bulk()
						Expect(err).ToNot(HaveOccurred())
						err = bulk.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						for {
							var timestamp time.Time
							if timestamp, err = bulk.NextRow(); err != nil {
								break
							}
							Expect(err).ToNot(HaveOccurred())
							Expect(timestamps[bulk.RowCount()]).To(Equal(timestamp))

							blobValue, err := bulk.GetBlob()
							Expect(err).ToNot(HaveOccurred())
							Expect(blobPoints[bulk.RowCount()].Content()).To(Equal(blobValue))

							doubleValue, err := bulk.GetDouble()
							Expect(err).ToNot(HaveOccurred())
							Expect(doublePoints[bulk.RowCount()].Content()).To(Equal(doubleValue))

							int64Value, err := bulk.GetInt64()
							Expect(err).ToNot(HaveOccurred())
							Expect(int64Points[bulk.RowCount()].Content()).To(Equal(int64Value))

							stringValue, err := bulk.GetString()
							Expect(err).ToNot(HaveOccurred())
							Expect(stringPoints[bulk.RowCount()].Content()).To(Equal(stringValue))

							timestampValue, err := bulk.GetTimestamp()
							Expect(err).ToNot(HaveOccurred())
							Expect(timestampPoints[bulk.RowCount()].Content()).To(Equal(timestampValue))

							symbolValue, err := bulk.GetSymbol()
							Expect(err).ToNot(HaveOccurred())
							Expect(symbolPoints[bulk.RowCount()].Content()).To(Equal(symbolValue))
						}
						Expect(err).To(Equal(ErrIteratorEnd))
						bulk.Release()
					})
					It("Should not work to get values when GetRanges has not been called", func() {
						bulk, err := timeseries.Bulk()
						Expect(err).ToNot(HaveOccurred())

						_, err = bulk.NextRow()
						Expect(err).To(HaveOccurred())
					})
					It("Should not work to get values in a non-correct order", func() {
						bulk, err := timeseries.Bulk()
						Expect(err).ToNot(HaveOccurred())

						err = bulk.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())

						_, err = bulk.NextRow()
						Expect(err).ToNot(HaveOccurred())

						_, err = bulk.GetDouble()
						Expect(err).To(HaveOccurred())
					})
				})
			})
			Context("Batch", func() {
				var (
					batchColumnsInfos []TsBatchColumnInfo
				)
				BeforeEach(func() {
					batchColumnsInfos = make([]TsBatchColumnInfo, 0)
					for _, col := range columnsInfo {
						batchColumnsInfos = append(batchColumnsInfos, TsBatchColumnInfo{alias, col.name, 10})
					}
				})
				Context("Init", func() {
					It("should not work with no columns", func() {
						_, err := handle.TsBatch()
						Expect(err).To(HaveOccurred())
					})
					It("should not work with a bad column", func() {
						newBatchColumnsInfos := make([]TsBatchColumnInfo, len(batchColumnsInfos))
						copy(newBatchColumnsInfos, batchColumnsInfos)
						newBatchColumnsInfos[0].Timeseries = "alias"
						_, err := handle.TsBatch(newBatchColumnsInfos...)
						Expect(err).To(HaveOccurred())
					})
				})
				Context("Initialized", func() {
					var (
						tsBatch *TsBatch
						err     error
					)
					JustBeforeEach(func() {
						err = nil
						tsBatch, err = handle.TsBatch(batchColumnsInfos...)
						Expect(err).ToNot(HaveOccurred())
					})
					AfterEach(func() {
						tsBatch.Release()
					})
					Context("Row", func() {
						var (
							blobValue      []byte    = []byte("content")
							doubleValue    float64   = 3.2
							int64Value     int64     = 2
							stringValue    string    = "content"
							timestampValue time.Time = time.Now()
							symbolValue    string    = "symbol"
						)
						It("should append all columns", func() {
							err = tsBatch.StartRow(timestampValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetBlob(blobIndex, blobValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetDouble(doubleIndex, doubleValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetInt64(int64Index, int64Value)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetString(stringIndex, stringValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetTimestamp(timestampIndex, timestampValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetSymbol(symbolIndex, symbolValue)
							Expect(err).ToNot(HaveOccurred())
						})
						It("should append columns and ignore fields", func() {
							err = tsBatch.StartRow(timestampValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetBlob(blobIndex, blobValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetDouble(doubleIndex, doubleValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetTimestamp(timestampIndex, timestampValue)
							Expect(err).ToNot(HaveOccurred())
						})
						It("should append columns on part of timeseries", func() {
							err = tsBatch.StartRow(timestampValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetBlob(blobIndex, blobValue)
							Expect(err).ToNot(HaveOccurred())
						})
						It("should fail to append columns - index past the end", func() {
							err = tsBatch.StartRow(timestampValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetTimestamp(timestampIndex+1, timestampValue)
							Expect(err).To(HaveOccurred())
						})
						Context("Push", func() {
							JustBeforeEach(func() {
								err = tsBatch.StartRow(timestampValue)
								Expect(err).ToNot(HaveOccurred())
								err := tsBatch.RowSetBlob(blobIndex, blobValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetDouble(doubleIndex, doubleValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetInt64(int64Index, int64Value)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetString(stringIndex, stringValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetTimestamp(timestampIndex, timestampValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetSymbol(symbolIndex, symbolValue)
								Expect(err).ToNot(HaveOccurred())
							})
							It("should push", func() {
								err = tsBatch.Push()
								Expect(err).ToNot(HaveOccurred())

								rg := NewRange(timestampValue, timestampValue.Add(5*time.Nanosecond))

								blobs, err := blobColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(blobs)).To(Equal(1))
								Expect(blobs[0].Content()).To(Equal(blobValue))

								doubles, err := doubleColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(doubles)).To(Equal(1))
								Expect(doubles[0].Content()).To(Equal(doubleValue))

								int64s, err := int64Column.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(int64s)).To(Equal(1))
								Expect(int64s[0].Content()).To(Equal(int64Value))

								strings, err := stringColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(strings)).To(Equal(1))
								Expect(strings[0].Content()).To(Equal(stringValue))

								nTimestamps, err := timestampColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(nTimestamps)).To(Equal(1))
								Expect(nTimestamps[0].Content().Equal(timestampValue)).To(BeTrue())

								symbols, err := symbolColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(symbols)).To(Equal(1))
								Expect(symbols[0].Content()).To(Equal(symbolValue))
							})
						})

						Context("Push Fast", func() {
							JustBeforeEach(func() {
								err = tsBatch.StartRow(timestampValue)
								Expect(err).ToNot(HaveOccurred())
								err := tsBatch.RowSetBlob(blobIndex, blobValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetDouble(doubleIndex, doubleValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetInt64(int64Index, int64Value)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetString(stringIndex, stringValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetTimestamp(timestampIndex, timestampValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetSymbol(symbolIndex, symbolValue)
								Expect(err).ToNot(HaveOccurred())
							})
							It("should push", func() {
								err = tsBatch.PushFast()
								Expect(err).ToNot(HaveOccurred())

								rg := NewRange(timestampValue, timestampValue.Add(5*time.Nanosecond))

								blobs, err := blobColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(blobs)).To(Equal(1))
								Expect(blobs[0].Content()).To(Equal(blobValue))

								doubles, err := doubleColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(doubles)).To(Equal(1))
								Expect(doubles[0].Content()).To(Equal(doubleValue))

								int64s, err := int64Column.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(int64s)).To(Equal(1))
								Expect(int64s[0].Content()).To(Equal(int64Value))

								strings, err := stringColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(strings)).To(Equal(1))
								Expect(strings[0].Content()).To(Equal(stringValue))

								nTimestamps, err := timestampColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(nTimestamps)).To(Equal(1))
								Expect(nTimestamps[0].Content().Equal(timestampValue)).To(BeTrue())

								symbols, err := symbolColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(symbols)).To(Equal(1))
								Expect(symbols[0].Content()).To(Equal(symbolValue))
							})
						})
					})
				})
			})
		})
	})
})