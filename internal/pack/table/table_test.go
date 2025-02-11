package table

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"

	_ "blockwatch.cc/knoxdb/internal/store/bolt"
	_ "blockwatch.cc/knoxdb/internal/store/mem"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	tests.RegisterEnum()
	m.Run()
}

func TestPackTable(t *testing.T) {
	tests.TestTableEngine[Table, *Table](t, "mem", "pack")
	tests.TestTableEngine[Table, *Table](t, "bolt", "pack")
}
