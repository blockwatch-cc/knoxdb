package table

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	etests "blockwatch.cc/knoxdb/internal/tests/engine"

	_ "blockwatch.cc/knoxdb/pkg/store/memdb"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	etests.RegisterEnum()
	m.Run()
}

func TestTable(t *testing.T) {
	etests.TestTableEngine[Table](t,
		engine.WithDriverType("mem"),
		engine.WithEngineType("pack"),
	)
}
