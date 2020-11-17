// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"runtime"
	"sync"

	"blockwatch.cc/knoxdb/util"
	"github.com/pierrec/lz4"
)

var (
	BlockEncoderPool = &sync.Pool{
		New: func() interface{} { return make([]byte, 0, BlockSizeHint) },
	}
	snappyWriterPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() interface{} { return NewSnappyWriter(nil) },
	)
	lz4WriterPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() interface{} { return lz4.NewWriter(nil) },
	)
	lz4ReaderPool = util.NewGenericPool(
		runtime.NumCPU(),
		func() interface{} { return lz4.NewReader(nil) },
	)

	int64Pool = &sync.Pool{
		New: func() interface{} { return make([]int64, 0, DefaultMaxPointsPerBlock) },
	}
	int32Pool = &sync.Pool{
		New: func() interface{} { return make([]int32, 0, DefaultMaxPointsPerBlock) },
	}
	int16Pool = &sync.Pool{
		New: func() interface{} { return make([]int16, 0, DefaultMaxPointsPerBlock) },
	}
	int8Pool = &sync.Pool{
		New: func() interface{} { return make([]int8, 0, DefaultMaxPointsPerBlock) },
	}
	uint64Pool = &sync.Pool{
		New: func() interface{} { return make([]uint64, 0, DefaultMaxPointsPerBlock) },
	}
	uint32Pool = &sync.Pool{
		New: func() interface{} { return make([]uint32, 0, DefaultMaxPointsPerBlock) },
	}
	uint16Pool = &sync.Pool{
		New: func() interface{} { return make([]uint16, 0, DefaultMaxPointsPerBlock) },
	}
	uint8Pool = &sync.Pool{
		New: func() interface{} { return make([]uint8, 0, DefaultMaxPointsPerBlock) },
	}
	boolPool = &sync.Pool{
		New: func() interface{} { return make([]bool, 0, DefaultMaxPointsPerBlock) },
	}
	float64Pool = &sync.Pool{
		New: func() interface{} { return make([]float64, 0, DefaultMaxPointsPerBlock) },
	}
	float32Pool = &sync.Pool{
		New: func() interface{} { return make([]float32, 0, DefaultMaxPointsPerBlock) },
	}
	stringPool = &sync.Pool{
		New: func() interface{} { return make([]string, 0, DefaultMaxPointsPerBlock) },
	}
	bytesPool = &sync.Pool{
		New: func() interface{} { return make([][]byte, 0, DefaultMaxPointsPerBlock) },
	}
)
