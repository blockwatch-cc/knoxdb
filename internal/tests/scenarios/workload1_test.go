// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// TestWorkload1 tests KnoxDB's handling of large transactions in a single-threaded environment.
// Ensures:
// - all rows are inserted in a single transaction.
// - data integrity is verified post-commit by streaming and comparing inserted records.

package scenarios

import (
	"context"
	"testing"

	tests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/stretchr/testify/require"
)

func TestWorkload1(t *testing.T) {
	// setup determinism
	SetupDeterministicRand(t)

	eng, cleanup := tests.NewDatabase(t, &tests.Types{})
	t.Cleanup(func() {
		cleanup()
		tests.SaveDatabaseFiles(t, eng)
	})
	db := knox.WrapEngine(eng)
	table, err := db.FindTable("types")
	require.NoError(t, err, "Missing table")

	ctx := context.Background()
	const txnSize = 100

	// Insert a large number of records in a single transaction
	data := make([]*tests.Types, txnSize)
	for i := 0; i < txnSize; i++ {
		data[i] = tests.NewRandomTypes(i)
	}
	startPK, _, err := table.Insert(ctx, data)
	require.NoError(t, err, "Failed to insert data")

	// Assign primary keys to records
	for i := range data {
		data[i].Id = startPK + uint64(i)
	}

	// Validate all rows are correctly inserted
	count := 0
	err = knox.NewGenericQuery[tests.Types]().
		WithTable(table).
		WithDebug(testing.Verbose()). // Enable detailed query logging
		Stream(ctx, func(res *tests.Types) error {
			require.NotEmpty(t, res.MyEnum, "Unexpected empty enum value %#v", res)
			// log.Infof("Streamed record: ID=%d, Int64=%d, MyEnum=%s", res.Id, res.Int64, res.MyEnum)
			require.Equal(t, data[count].Id, res.Id, "Record ID mismatch")
			require.Equal(t, data[count].Int64, res.Int64, "Int64 mismatch")
			require.Equal(t, data[count].MyEnum, res.MyEnum, "Enum mismatch")
			count++
			return nil
		})
	require.NoError(t, err, "Failed to stream data")
	require.Equal(t, txnSize, count, "Row count mismatch")

	for _, v := range data {
		var res tests.Types
		_, err := knox.NewGenericQuery[tests.Types]().
			WithTable(table).
			AndEqual("int64", v.Int64).
			Execute(ctx, &res)
		require.NoError(t, err)
		require.Greater(t, v.Id, uint64(0))
		require.Equal(t, v.Int64, res.Int64)
	}
}
