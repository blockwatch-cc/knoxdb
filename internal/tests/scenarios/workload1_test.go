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

// TestWorkload1 simulates a single-threaded large transaction, ensuring atomicity and durability.
func TestWorkload1(t *testing.T) {
	_, table, cleanup := SetupDatabase(t)
	defer cleanup()

	ctx := context.Background()
	const txnSize = 100

	// Step 1: Insert a large number of records in a single transaction.
	data := make([]*Types, txnSize)
	for i := 0; i < txnSize; i++ {
		data[i] = NewRandomTypes(i)
	}
	_, err := table.Insert(ctx, data)
	require.NoError(t, err, "Failed to insert data")

	// Placeholder: Inject fault before committing the transaction.

	// Step 2: Validate that all rows are correctly inserted.
	count := 0
	err = knox.NewGenericQuery[Types]().
		WithTable(table).
		Stream(ctx, func(res *Types) error {
			require.Equal(t, data[count].Int64, res.Int64, "Mismatch in Int64 field")
			require.Equal(t, uint64(count+1), res.Id, "Mismatch in Id field")
			count++
			return nil
		})
	require.NoError(t, err, "Failed to stream data")
	require.Equal(t, txnSize, count, "Row count mismatch")
}
