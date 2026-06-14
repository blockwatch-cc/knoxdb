package types

import (
	"blockwatch.cc/knoxdb/pkg/schema"
)

type Compression = schema.Compression

const (
	CompressNone   = schema.Uncompressed
	CompressSnappy = schema.Snappy
	CompressLZ4    = schema.LZ4
	CompressZstd   = schema.Zstd
)

type FieldType = schema.FieldType

const (
	FT_TIMESTAMP = schema.Timestamp  // 1
	FT_DURATION  = schema.Duration   // 2
	FT_DATE      = schema.Date       // 3
	FT_TIME      = schema.Time       // 4
	FT_U64       = schema.Uint64     // 5
	FT_U32       = schema.Uint32     // 6
	FT_U16       = schema.Uint16     // 7
	FT_U8        = schema.Uint8      // 8
	FT_I64       = schema.Int64      // 9
	FT_I32       = schema.Int32      // 10
	FT_I16       = schema.Int16      // 11
	FT_I8        = schema.Int8       // 12
	FT_BOOL      = schema.Boolean    // 13
	FT_F64       = schema.Float64    // 14
	FT_F32       = schema.Float32    // 15
	FT_I256      = schema.Int256     // 16
	FT_I128      = schema.Int128     // 17
	FT_D256      = schema.Decimal256 // 18
	FT_D128      = schema.Decimal128 // 19
	FT_D64       = schema.Decimal64  // 20
	FT_D32       = schema.Decimal32  // 21
	FT_BIGINT    = schema.Bigint     // 22
	FT_STRING    = schema.String     // 23
	FT_TEXT      = schema.Text       // 24
	FT_BYTES     = schema.Bytes      // 25
	FT_BINARY    = schema.Binary     // 26
	FT_LIST      = schema.List       // 27
	FT_MAP       = schema.Map        // 28
	FT_UNION     = schema.Union      // 29
	FT_VARIANT   = schema.Variant    // 30
	FT_ENUM      = schema.Enum       // 31
)

type FieldFlags = schema.FieldFlags

const (
	F_PRIMARY  = schema.FlagPrimary
	F_DELETED  = schema.FlagDeleted
	F_METADATA = schema.FlagMetadata
	F_NULLABLE = schema.FlagNullable
	F_TIMEBASE = schema.FlagTimebase
	F_ACTION   = schema.FlagAction
)

type TimeScale = schema.TimeScale

type FilterType = schema.FilterType

const (
	FL_NONE    = schema.NoFilter
	FL_BITS    = schema.BitsFilter
	FL_BLOOM2B = schema.BloomFilter2b
	FL_BLOOM3B = schema.BloomFilter3b
	FL_BLOOM4B = schema.BloomFilter4b
	FL_BLOOM5B = schema.BloomFilter5b
	FL_BFUSE8  = schema.BinaryFuseFilter8
	FL_BFUSE16 = schema.BinaryFuseFilter16
)

type IndexType = schema.IndexType

const (
	IT_NONE      = schema.InvalidIndex
	IT_HASH      = schema.HashIndex
	IT_INT       = schema.IntegerIndex
	IT_PK        = schema.PrimaryKeyIndex
	IT_COMPOSITE = schema.CompositeIndex
)
