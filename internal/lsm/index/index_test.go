package index

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/lsm/table"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"

	_ "blockwatch.cc/knoxdb/internal/store/badger"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	tests.RegisterEnum()
	m.Run()
}

func TestIndex(t *testing.T) {
	tableEngine := table.NewTable()
	tests.TestIndexEngine[Index, *Index](t, "badger", "lsm", tableEngine, []types.IndexType{
		types.IndexTypeInt,
		types.IndexTypeHash,
	})
}

func TestIndexComposite(t *testing.T) {
	tableEngine := table.NewTable()
	tests.TestCompositeIndexEngine[Index, *Index](t, "badger", "lsm", tableEngine)
}
