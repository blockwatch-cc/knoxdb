package index

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/pack/table"
	etests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/internal/types"

	_ "blockwatch.cc/knoxdb/internal/store/bolt"
	_ "blockwatch.cc/knoxdb/internal/store/mem"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	etests.RegisterEnum()
	m.Run()
}

func TestIndex(t *testing.T) {
	typs := []types.IndexType{
		types.IndexTypeInt,
		types.IndexTypeHash,
	}
	etests.TestIndexEngine[Index, *Index](t, "mem", "pack", table.NewTable(), typs)
	etests.TestIndexEngine[Index, *Index](t, "bolt", "pack", table.NewTable(), typs)
}

func TestIndexComposite(t *testing.T) {
	etests.TestCompositeIndexEngine[Index, *Index](t, "mem", "pack", table.NewTable())
	etests.TestCompositeIndexEngine[Index, *Index](t, "bolt", "pack", table.NewTable())
}
