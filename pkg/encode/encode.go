package encode

import (
	"blockwatch.cc/knoxdb/internal/encode"
)

// export definitions to use by external tooling

type (
	ContainerType = encode.ContainerType

	ContextInt64   = encode.Context[int64]
	ContextFloat64 = encode.Context[float64]

	ContainerInt64   = encode.NumberContainer[int64]
	ContainerFloat64 = encode.NumberContainer[float64]
)

const (
	TIntConstant   = encode.TIntConstant
	TIntDelta      = encode.TIntDelta
	TIntRunEnd     = encode.TIntRunEnd
	TIntBitpacked  = encode.TIntBitpacked
	TIntDictionary = encode.TIntDictionary
	TIntSimple8    = encode.TIntSimple8
	TIntRaw        = encode.TIntRaw

	TFloatConstant   = encode.TFloatConstant
	TFloatRunEnd     = encode.TFloatRunEnd
	TFloatDictionary = encode.TFloatDictionary
	TFloatAlp        = encode.TFloatAlp
	TFloatAlpRd      = encode.TFloatAlpRd
	TFloatRaw        = encode.TFloatRaw

	TStringConstant   = encode.TStringConstant
	TStringFixed      = encode.TStringFixed
	TStringCompact    = encode.TStringCompact
	TStringDictionary = encode.TStringDictionary

	TBitmapZero   = encode.TBitmapZero
	TBitmapOne    = encode.TBitmapOne
	TBitmapDense  = encode.TBitmapDense
	TBitmapSparse = encode.TBitmapSparse
)

var (
	NewInt64 = encode.NewInt[int64]

	AnalyzeInt64 = encode.AnalyzeInt[int64]

	EncodeInt64 = encode.EncodeInt[int64]

	NewFloat64 = encode.NewFloat[float64]

	AnalyzeFloat64 = encode.AnalyzeFloat[float64]

	EncodeFloat64 = encode.EncodeFloat[float64]
)
