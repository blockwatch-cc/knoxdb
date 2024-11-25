package table

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"

	_ "blockwatch.cc/knoxdb/internal/store/bolt"
)

func TestTable(t *testing.T) {
	tests.TestTableEngine[Table, *Table](t, "badger")
}
