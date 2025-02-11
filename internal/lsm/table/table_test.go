package table

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"

	_ "blockwatch.cc/knoxdb/internal/store/badger"
	_ "blockwatch.cc/knoxdb/internal/store/mem"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	tests.RegisterEnum()
	m.Run()
}

func TestLSMTable(t *testing.T) {
	// FIXME
	// tests.TestTableEngine[Table, *Table](t, "mem", "lsm")
	tests.TestTableEngine[Table, *Table](t, "badger", "lsm")
}
