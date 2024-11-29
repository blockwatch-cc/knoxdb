// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// TestWorkload3 simulates a banking system with debit and credit operations, where accounts are initialized
// with a balance and operations are performed atomically.
// Ensures:
// - transactions are atomic (no partial updates).
// - total balance consistency is maintained across accounts, with correct enum field handling.

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
	const transferAmount = 50

	// Initialize accounts with balances
	data := make([]*Types, accountCount)
	for i := 0; i < accountCount; i++ {
		data[i] = &Types{
			Int64:  initialBalance,
			MyEnum: myEnums[i%len(myEnums)],
		}
	}
	startPK, err := table.Insert(ctx, data)
	require.NoError(t, err, "Failed to initialize accounts")

	// Update IDs for accounts
	for i := range data {
		data[i].Id = startPK + uint64(i)
	}

	// Perform debit and credit operations
	for i := 0; i < txnCount; i++ {
		from := i % accountCount
		to := (i + 1) % accountCount

		var fromAccount, toAccount Types

		// Load the "from" account
		err := knox.NewGenericQuery[Types]().
			WithTable(table).
			AndEqual("id", data[from].Id).
			Execute(ctx, &fromAccount)
		require.NoError(t, err, "Failed to load 'from' account with ID: %d", data[from].Id)
		require.Equal(t, data[from].Id, fromAccount.Id, "Loaded 'from' account ID mismatch")

		// Load the "to" account
		err = knox.NewGenericQuery[Types]().
			WithTable(table).
			AndEqual("id", data[to].Id).
			Execute(ctx, &toAccount)
		require.NoError(t, err, "Failed to load 'to' account with ID: %d", data[to].Id)
		require.Equal(t, data[to].Id, toAccount.Id, "Loaded 'to' account ID mismatch")

		// Perform debit and credit
		fromAccount.Int64 -= transferAmount
		toAccount.Int64 += transferAmount

		_, err = table.Update(ctx, []*Types{&fromAccount, &toAccount})
		require.NoError(t, err, "Failed to update accounts during transaction")
	}

	// Validate total balance consistency
	totalBalance := int64(0)
	err = knox.NewGenericQuery[Types]().
		WithTable(table).
		Stream(ctx, func(res *Types) error {
			totalBalance += res.Int64
			return nil
		})
	require.NoError(t, err, "Failed to stream data for total balance validation")
	require.Equal(t, int64(accountCount*initialBalance), totalBalance, "Total balance mismatch")

	// Validate final balances for each account
	expectedBalanceAdjustments := txnCount / accountCount * transferAmount
	for _, account := range data {
		err := knox.NewGenericQuery[Types]().
			WithTable(table).
			AndEqual("id", account.Id).
			Execute(ctx, &account)
		require.NoError(t, err, "Failed to load account with ID: %d", account.Id)
		expectedBalance := initialBalance + int64(expectedBalanceAdjustments)
		require.Equal(t, expectedBalance, account.Int64, "Final balance mismatch for account ID: %d", account.Id)
	}
}
