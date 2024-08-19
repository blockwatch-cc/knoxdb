// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

type FieldType byte

const (
	FieldTypeInvalid FieldType = iota
	FieldTypeDatetime
	FieldTypeInt64
	FieldTypeUint64
	FieldTypeFloat64
	FieldTypeBoolean
	FieldTypeString
	FieldTypeBytes
	FieldTypeInt32
	FieldTypeInt16
	FieldTypeInt8
	FieldTypeUint32
	FieldTypeUint16
	FieldTypeUint8
	FieldTypeFloat32
	FieldTypeInt256
	FieldTypeInt128
	FieldTypeDecimal256
	FieldTypeDecimal128
	FieldTypeDecimal64
	FieldTypeDecimal32
)

var (
	fieldTypeString         = "_datetime_int64_uint64_float64_boolean_string_bytes_int32_int16_int8_uint32_uint16_uint8_float32_int256_int128_decimal256_decimal128_decimal64_decimal32"
	fieldTypeIdx            = [...][2]int{{0, 1}, {1, 9}, {10, 15}, {16, 22}, {23, 30}, {31, 38}, {39, 45}, {46, 51}, {52, 57}, {58, 63}, {64, 68}, {69, 75}, {76, 82}, {83, 88}, {89, 96}, {97, 103}, {104, 110}, {111, 121}, {122, 132}, {133, 142}, {143, 152}}
	fieldTypeReverseStrings = map[string]FieldType{}

	fieldTypeDataSize = [...]int{
		FieldTypeInvalid:    0,
		FieldTypeDatetime:   8,
		FieldTypeInt64:      8,
		FieldTypeUint64:     8,
		FieldTypeFloat64:    8,
		FieldTypeBoolean:    1,
		FieldTypeString:     16,
		FieldTypeBytes:      24,
		FieldTypeInt32:      4,
		FieldTypeInt16:      2,
		FieldTypeInt8:       1,
		FieldTypeUint32:     4,
		FieldTypeUint16:     2,
		FieldTypeUint8:      1,
		FieldTypeFloat32:    4,
		FieldTypeInt256:     32,
		FieldTypeInt128:     16,
		FieldTypeDecimal256: 32,
		FieldTypeDecimal128: 16,
		FieldTypeDecimal64:  8,
		FieldTypeDecimal32:  4,
	}

	fieldTypeWireSize = [...]int{
		FieldTypeInvalid:    0,
		FieldTypeDatetime:   8,
		FieldTypeInt64:      8,
		FieldTypeUint64:     8,
		FieldTypeFloat64:    8,
		FieldTypeBoolean:    1,
		FieldTypeString:     4, // minimum uint32 for size
		FieldTypeBytes:      4, // minimum uint32 for size
		FieldTypeInt32:      4,
		FieldTypeInt16:      2,
		FieldTypeInt8:       1,
		FieldTypeUint32:     4,
		FieldTypeUint16:     2,
		FieldTypeUint8:      1,
		FieldTypeFloat32:    4,
		FieldTypeInt256:     32,
		FieldTypeInt128:     16,
		FieldTypeDecimal256: 32,
		FieldTypeDecimal128: 16,
		FieldTypeDecimal64:  8,
		FieldTypeDecimal32:  4,
	}
)

func init() {
	for i, v := range fieldTypeIdx {
		fieldTypeReverseStrings[fieldTypeString[v[0]:v[1]]] = FieldType(i)
	}
}

func (t FieldType) IsValid() bool {
	return t != FieldTypeInvalid
}

func (t FieldType) String() string {
	idx := fieldTypeIdx[t]
	return fieldTypeString[idx[0]:idx[1]]
}

func (t FieldType) Size() int {
	if int(t) < len(fieldTypeWireSize) {
		return fieldTypeWireSize[t]
	}
	return 0
}

type FieldFlags byte

const (
	FieldFlagPrimary FieldFlags = 1 << iota
	FieldFlagIndexed
	FieldFlagDeleted
	FieldFlagInternal
)

func (i FieldFlags) Is(f FieldFlags) bool {
	return i&f > 0
}

type FieldCompression byte

const (
	FieldCompressNone FieldCompression = iota
	FieldCompressSnappy
	FieldCompressLZ4
	FieldCompressZstd
)

func (i FieldCompression) Is(f FieldCompression) bool {
	return i&f > 0
}

type IndexKind byte

const (
	IndexKindNone IndexKind = iota
	IndexKindHash
	IndexKindInt
	IndexKindComposite
	IndexKindBloom
	IndexKindBfuse
	IndexKindBits
)

func (i IndexKind) Is(f IndexKind) bool {
	return i&f > 0
}

type IfaceFlags byte

const (
	IfaceBinaryMarshaler IfaceFlags = 1 << iota
	IfaceBinaryUnmarshaler
	IfaceTextMarshaler
	IfaceTextUnmarshaler
	IfaceStringer
)

func (i IfaceFlags) Is(f IfaceFlags) bool {
	return i&f > 0
}
