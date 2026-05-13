// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build wasm

package knox

import (
	// import stores
	_ "blockwatch.cc/knoxdb/pkg/store/memdb"

	// import table engines
	// _ "blockwatch.cc/knoxdb/internal/lsm/table"
	_ "blockwatch.cc/knoxdb/internal/pack/table"

	// import index engines
	// _ "blockwatch.cc/knoxdb/internal/lsm/index"
	_ "blockwatch.cc/knoxdb/internal/pack/index"
)
