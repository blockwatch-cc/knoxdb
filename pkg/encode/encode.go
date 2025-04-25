package encode

import (
	"blockwatch.cc/knoxdb/internal/encode"
)

// export definitions to use by external tooling

type (
	ContainerTypeInteger = encode.IntegerContainerType
	ContainerTypeFloat   = encode.FloatContainerType

	ContextInt64   = encode.IntegerContext[int64]
	ContextFloat64 = encode.FloatContext[float64]

	ContainerInt64   = encode.IntegerContainer[int64]
	ContainerFloat64 = encode.FloatContainer[float64]
)

const (
	TIntegerConstant   = encode.TIntegerConstant
	TIntegerDelta      = encode.TIntegerDelta
	TIntegerRunEnd     = encode.TIntegerRunEnd
	TIntegerBitpacked  = encode.TIntegerBitpacked
	TIntegerDictionary = encode.TIntegerDictionary
	TIntegerSimple8    = encode.TIntegerSimple8
	TIntegerRaw        = encode.TIntegerRaw

	TFloatConstant   = encode.TFloatConstant
	TFloatRunEnd     = encode.TFloatRunEnd
	TFloatDictionary = encode.TFloatDictionary
	TFloatAlp        = encode.TFloatAlp
	TFloatAlpRd      = encode.TFloatAlpRd
	TFloatRaw        = encode.TFloatRaw
)

var (
	NewInt64 = encode.NewInt[int64]

	AnalyzeInt64 = encode.AnalyzeInt[int64]

	EncodeInt64 = encode.EncodeInt[int64]

	NewFloat64 = encode.NewFloat[float64]

	AnalyzeFloat64 = encode.AnalyzeFloat[float64]

	EncodeFloat64 = encode.EncodeFloat[float64]
)
