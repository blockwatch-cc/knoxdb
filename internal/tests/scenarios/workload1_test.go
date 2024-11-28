// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// Workload1 tests KnoxDB's handling of large transactions in a single-threaded environment.
// Ensures:
// - all rows are inserted in a single transaction.
// - data integrity is verified post-commit.

package scenarios

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/stretchr/testify/require"
)

func TestWorkload1(t *testing.T) {
	_, table, cleanup := SetupDatabase(t)
	defer cleanup()

	ctx := context.Background()
	const txnSize = 100

	// Insert a large number of records in a single transaction
	data := make([]*Types, txnSize)
	for i := 0; i < txnSize; i++ {
		data[i] = NewRandomTypes(i)
	}
	startPK, err := table.Insert(ctx, data)
	require.NoError(t, err, "Failed to insert data")

	// Assign primary keys to records
	for i := range data {
		data[i].Id = startPK + uint64(i)
	}

	// Validate all rows are correctly inserted
	count := 0
	err = knox.NewGenericQuery[Types]().
		WithTable(table).
		WithDebug(true). // Enable detailed query logging
		Stream(ctx, func(res *Types) error {
			require.NotEmpty(t, res.MyEnum, "Unexpected empty enum value")
			require.Equal(t, data[count].Id, res.Id, "Record ID mismatch")
			require.Equal(t, data[count].Int64, res.Int64, "Int64 mismatch")
			require.Equal(t, data[count].MyEnum, res.MyEnum, "Enum mismatch")
			count++
			return nil
		})
	require.NoError(t, err, "Failed to stream data")
	require.Equal(t, txnSize, count, "Row count mismatch")
}
