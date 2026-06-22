package index

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack/table"
	etests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/internal/types"

	_ "blockwatch.cc/knoxdb/pkg/store/boltdb"
	_ "blockwatch.cc/knoxdb/pkg/store/memdb"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	etests.RegisterEnum()
	m.Run()
}

func TestIndex(t *testing.T) {
	typs := []types.IndexType{
		types.IT_INT,
		types.IT_HASH,
	}
	etests.TestIndexEngine[Index](t, table.NewTable(), typs,
		engine.WithDriverType("mem"),
		engine.WithEngineType("pack"),
	)
	etests.TestIndexEngine[Index](t, table.NewTable(), typs,
		engine.WithDriverType("bolt"),
		engine.WithEngineType("pack"),
	)
}

func TestIndexComposite(t *testing.T) {
	etests.TestCompositeIndexEngine[Index](t, table.NewTable(),
		engine.WithDriverType("mem"),
		engine.WithEngineType("pack"),
	)
	etests.TestCompositeIndexEngine[Index](t, table.NewTable(),
		engine.WithDriverType("bolt"),
		engine.WithEngineType("pack"),
	)
}
