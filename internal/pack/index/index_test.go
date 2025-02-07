package index

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/pack/table"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"

	_ "blockwatch.cc/knoxdb/internal/store/bolt"
	_ "blockwatch.cc/knoxdb/internal/store/mem"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	tests.RegisterEnum()
	m.Run()
}

func TestIndex(t *testing.T) {
	typs := []types.IndexType{
		types.IndexTypeInt,
		types.IndexTypeHash,
	}
	tests.TestIndexEngine[Index, *Index](t, "mem", "pack", table.NewTable(), typs)
	tests.TestIndexEngine[Index, *Index](t, "bolt", "pack", table.NewTable(), typs)
}

func TestIndexComposite(t *testing.T) {
	tests.TestCompositeIndexEngine[Index, *Index](t, "mem", "pack", table.NewTable())
	tests.TestCompositeIndexEngine[Index, *Index](t, "bolt", "pack", table.NewTable())
}
