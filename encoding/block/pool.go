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
	BlockPool = &sync.Pool{
		New: func() interface{} { return &Block{} },
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
)
