package qdb

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// arrowTypeExpectedOf is the forward mapping as the reader and query paths
// emit it. It is the one fact a generative test cannot derive, so it is
// written out here and everything else is checked against it.
var arrowTypeExpectedOf = map[TsColumnType]arrow.DataType{
	TsColumnInt64:     arrow.PrimitiveTypes.Int64,
	TsColumnDouble:    arrow.PrimitiveTypes.Float64,
	TsColumnTimestamp: &arrow.TimestampType{Unit: arrow.Nanosecond},
	TsColumnBlob:      arrow.BinaryTypes.Binary,
	TsColumnString:    arrow.BinaryTypes.String,
	TsColumnSymbol:    arrow.BinaryTypes.String,
}

// TestColumnTypeArrowRoundTrip checks the invariant the REST reader relies
// on: for every valid column type, ArrowType answers exactly the type the C
// side emits, as one stable instance, and ColumnTypeOfArrow inverts it,
// with Symbol collapsing to String since utf8 cannot name a symbol. An
// invalid column type answers nil.
func TestColumnTypeArrowRoundTrip(t *testing.T) {
	assert.Nil(t, TsColumnUninitialized.ArrowType())
	assert.Nil(t, TsColumnType(999).ArrowType())

	rapid.Check(t, func(rt *rapid.T) {
		ctype := rapid.SampledFrom(TsColumnTypes).Draw(rt, "ctype")

		got := ctype.ArrowType()
		require.NotNil(rt, got)
		assert.True(rt, arrow.TypeEqual(arrowTypeExpectedOf[ctype], got), "want %v, got %v", arrowTypeExpectedOf[ctype], got)
		assert.Same(rt, got, ctype.ArrowType(), "answers are one instance")

		back, err := ColumnTypeOfArrow(got)
		require.NoError(rt, err)
		if ctype == TsColumnSymbol {
			assert.Equal(rt, TsColumnString, back)
		} else {
			assert.Equal(rt, ctype, back)
		}
	})
}

// arrowVerdict pairs an Arrow type with what ColumnTypeOfArrow must answer
// for it: a column type, or TsColumnUninitialized when it is refused.
type arrowVerdict struct {
	dt   arrow.DataType
	want TsColumnType
}

// genAcceptedArrowType draws a type the writer accepts: the four fixed ones,
// date64, or a timestamp of any unit with or without a zone.
func genAcceptedArrowType() *rapid.Generator[arrowVerdict] {
	fixed := []arrowVerdict{
		{arrow.PrimitiveTypes.Int64, TsColumnInt64},
		{arrow.PrimitiveTypes.Float64, TsColumnDouble},
		{arrow.BinaryTypes.String, TsColumnString},
		{arrow.BinaryTypes.Binary, TsColumnBlob},
		{arrow.PrimitiveTypes.Date64, TsColumnTimestamp},
	}
	timestamp := rapid.Custom(func(rt *rapid.T) arrowVerdict {
		unit := rapid.SampledFrom([]arrow.TimeUnit{arrow.Second, arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond}).Draw(rt, "unit")
		zone := rapid.SampledFrom([]string{"", "UTC", "Europe/Paris", "+05:30"}).Draw(rt, "zone")

		return arrowVerdict{&arrow.TimestampType{Unit: unit, TimeZone: zone}, TsColumnTimestamp}
	})

	return rapid.OneOf(rapid.SampledFrom(fixed), timestamp)
}

// genRejectedArrowType draws a type the writer refuses: every width or
// encoding that could be widened into an accepted one, and structural types.
func genRejectedArrowType() *rapid.Generator[arrowVerdict] {
	rejected := []arrow.DataType{
		nil,
		arrow.Null,
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Date32,
		arrow.FixedWidthTypes.Duration_ns,
		arrow.BinaryTypes.LargeString,
		arrow.BinaryTypes.LargeBinary,
		&arrow.FixedSizeBinaryType{ByteWidth: 16},
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String},
		arrow.ListOf(arrow.PrimitiveTypes.Int64),
		arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}),
	}

	return rapid.Custom(func(rt *rapid.T) arrowVerdict {
		return arrowVerdict{rapid.SampledFrom(rejected).Draw(rt, "rejected"), TsColumnUninitialized}
	})
}

// TestColumnTypeOfArrow checks the invariant the writer relies on: an Arrow
// type is either stored as exactly one column type, or refused with
// ErrIncompatibleType and never widened. The two generators partition the
// types the test knows about, so a type must not change side without this
// test changing with it.
func TestColumnTypeOfArrow(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		v := rapid.OneOf(genAcceptedArrowType(), genRejectedArrowType()).Draw(rt, "verdict")

		got, err := ColumnTypeOfArrow(v.dt)
		assert.Equal(rt, v.want, got, "%v", v.dt)
		if v.want == TsColumnUninitialized {
			require.Error(rt, err, "%v", v.dt)
			assert.True(rt, errors.Is(err, ErrIncompatibleType), "%v: %v", v.dt, err)
		} else {
			require.NoError(rt, err, "%v", v.dt)
		}
	})
}
