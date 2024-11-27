// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// Workload3 simulates a banking system with debit and credit operations.
// Ensures:
// - transactions are atomic (no partial updates).
// - total balance consistency is maintained across accounts.

package scenarios

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/stretchr/testify/require"
)

func TestWorkload3(t *testing.T) {
	_, table, cleanup := SetupDatabase(t)
	defer cleanup()

	ctx := context.Background()
	const accountCount = 10
	const txnCount = 20
	const initialBalance = 100

	// Step 1: Initialize accounts with balances
	data := make([]*Types, accountCount)
	for i := 0; i < accountCount; i++ {
		data[i] = &Types{
			Int64: initialBalance, // Set initial balance, omit setting ID
		}
	}
	_, err := table.Insert(ctx, data)
	require.NoError(t, err, "Failed to initialize accounts")

	// Step 2: Perform debit and credit operations
	for i := 0; i < txnCount; i++ {
		from := i % accountCount
		to := (i + 1) % accountCount

		var fromAccount, toAccount Types

		// Load the "from" account and validate
		err := knox.NewGenericQuery[Types]().
			WithTable(table).
			AndEqual("id", uint64(from+1)).
			Execute(ctx, &fromAccount)
		require.NoError(t, err, "Failed to load 'from' account")
		require.Equal(t, uint64(from+1), fromAccount.Id)

		// Load the "to" account and validate
		err = knox.NewGenericQuery[Types]().
			WithTable(table).
			AndEqual("id", uint64(to+1)).
			Execute(ctx, &toAccount)
		require.NoError(t, err, "Failed to load 'to' account")
		require.Equal(t, uint64(to+1), toAccount.Id)

		// Perform the debit and credit
		fromAccount.Int64 -= 50
		toAccount.Int64 += 50

		_, err = table.Update(ctx, []*Types{&fromAccount, &toAccount})
		require.NoError(t, err, "Failed to update accounts")
	}

	// Step 3: Validate total balance consistency
	totalBalance := int64(0)
	err = knox.NewGenericQuery[Types]().
		WithTable(table).
		Stream(ctx, func(res *Types) error {
			totalBalance += res.Int64
			return nil
		})
	require.NoError(t, err, "Failed to stream data")
	require.Equal(t, int64(accountCount*initialBalance), totalBalance, "Total balance mismatch")
}
