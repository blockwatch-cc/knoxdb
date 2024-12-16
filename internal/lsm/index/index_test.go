package index

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/lsm/table"
	"blockwatch.cc/knoxdb/internal/tests"

	_ "blockwatch.cc/knoxdb/internal/store/badger"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	tests.RegisterEnum()
	m.Run()
}

func TestIndexHash(t *testing.T) {
	tableEngine := table.NewTable()
	tests.TestIndexEngine[Index, *Index](t, "badger", "lsm", tableEngine)
}
