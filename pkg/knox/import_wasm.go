// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	// import stores
	_ "blockwatch.cc/knoxdb/internal/store/mem"

	// import table engines
	_ "blockwatch.cc/knoxdb/internal/table/lsm"
	_ "blockwatch.cc/knoxdb/internal/table/pack"

	// import index engines
	_ "blockwatch.cc/knoxdb/internal/index/lsm"
	_ "blockwatch.cc/knoxdb/internal/index/pack"

	// import store engines
	_ "blockwatch.cc/knoxdb/internal/kvstore"
)
