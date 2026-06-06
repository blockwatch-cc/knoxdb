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
	FT_TIMESTAMP = schema.Timestamp
	FT_I8        = schema.Int8
	FT_I16       = schema.Int16
	FT_I32       = schema.Int32
	FT_I64       = schema.Int64
	FT_I128      = schema.Int128
	FT_I256      = schema.Int256
	FT_U8        = schema.Uint8
	FT_U16       = schema.Uint16
	FT_U32       = schema.Uint32
	FT_U64       = schema.Uint64
	FT_F32       = schema.Float32
	FT_F64       = schema.Float64
	FT_D32       = schema.Decimal32
	FT_D64       = schema.Decimal64
	FT_D128      = schema.Decimal128
	FT_D256      = schema.Decimal256
	FT_BOOL      = schema.Boolean
	FT_STRING    = schema.String
	FT_BYTES     = schema.Bytes
	FT_BIGINT    = schema.Bigint
	FT_TIME      = schema.Time
	FT_DATE      = schema.Date
	FT_TEXT      = schema.Text
	FT_BLOB      = schema.Binary
	FT_LIST      = schema.List
	FT_MAP       = schema.Map
)

type FieldFlags = schema.FieldFlags

const (
	F_PRIMARY  = schema.FlagPrimary
	F_ARRAY    = schema.FlagArray
	F_ENUM     = schema.FlagEnum
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
