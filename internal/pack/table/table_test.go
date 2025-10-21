package table

import (
	"testing"

	etests "blockwatch.cc/knoxdb/internal/tests/engine"

	_ "blockwatch.cc/knoxdb/pkg/store/bolt"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	etests.RegisterEnum()
	m.Run()
}

func TestTable(t *testing.T) {
	etests.TestTableEngine[Table, *Table](t, "bolt", "pack")
}
