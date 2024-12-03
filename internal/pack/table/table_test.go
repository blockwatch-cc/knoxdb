package table

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"

	_ "blockwatch.cc/knoxdb/internal/store/bolt"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	tests.RegisterEnum()
	m.Run()
}

func TestTable(t *testing.T) {
	tests.TestTableEngine[Table, *Table](t, "bolt", "pack")
}
