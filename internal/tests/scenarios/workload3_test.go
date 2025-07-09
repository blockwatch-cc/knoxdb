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

	tests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/stretchr/testify/require"
)

// Ledger defines a simple banking account ledger.
type Ledger struct {
	Id      uint64 `knox:"id,pk"`
	Balance int64  `knox:"balance"`
}

func TestWorkload3(t *testing.T) {
	t.Setenv("KNOX_DRIVER", "mem")
	eng, cleanup := tests.NewDatabase(t, &Ledger{})
	t.Cleanup(func() {
		cleanup()
		tests.SaveDatabaseFiles(t, eng)
	})
	db := knox.WrapEngine(eng)
	table, err := db.UseTable("ledger")
	require.NoError(t, err, "Missing table")

	ctx := context.Background()
	const numAccounts = 100
	const numTransfersPerTx = 10
	const initialBalance int64 = 100

	// Initialize accounts with balances
	data := make([]*Ledger, numAccounts)
	for i := 0; i < numAccounts; i++ {
		data[i] = &Ledger{
			Balance: initialBalance,
		}
	}
	startPK, err := table.Insert(ctx, data)
	require.NoError(t, err, "Failed to initialize accounts")

	// Update IDs for accounts
	for i := range data {
		data[i].Id = startPK + uint64(i)
	}

	// Perform debit and credit operations
	for i := 0; i < numAccounts/(numTransfersPerTx*2); i++ {
		func(txId int) {
			ctx, commit, abort, err := db.Begin(ctx)
			require.NoError(t, err, "Begin tx failed")
			defer abort()
			for k := 0; k < numTransfersPerTx; k++ {
				from := txId*numTransfersPerTx*2 + 2*k
				to := txId*numTransfersPerTx*2 + 2*k + 1

				var fromAccount, toAccount Ledger

				// Load the "from" account
				err := knox.NewGenericQuery[Ledger]().
					WithTable(table).
					AndEqual("id", data[from].Id).
					Execute(ctx, &fromAccount)
				require.NoError(t, err, "Failed to load 'from' account with ID: %d", data[from].Id)
				require.Equal(t, data[from].Id, fromAccount.Id, "Loaded 'from' account ID mismatch")

				// Load the "to" account
				err = knox.NewGenericQuery[Ledger]().
					WithTable(table).
					AndEqual("id", data[to].Id).
					Execute(ctx, &toAccount)
				require.NoError(t, err, "Failed to load 'to' account with ID: %d", data[to].Id)
				require.Equal(t, data[to].Id, toAccount.Id, "Loaded 'to' account ID mismatch")

				// Perform debit and credit
				amount := fromAccount.Balance / 2
				fromAccount.Balance -= amount
				toAccount.Balance += amount

				// t.Logf("Send %d from %d to %d => [%d]=%d [%d]=%d",
				// 	amount,
				// 	fromAccount.Id,
				// 	toAccount.Id,
				// 	fromAccount.Id,
				// 	fromAccount.Balance,
				// 	toAccount.Id,
				// 	toAccount.Balance,
				// )

				_, err = table.Update(ctx, []*Ledger{&fromAccount, &toAccount})
				require.NoError(t, err, "Failed to update accounts during transaction")
			}
			require.NoError(t, commit())
		}(i)
	}

	// Validate total balance consistency and individual account
	totalBalance := int64(0)
	err = knox.NewGenericQuery[Ledger]().
		WithTable(table).
		Stream(ctx, func(res *Ledger) error {
			if res.Id%2 == 1 {
				// sender
				require.Equal(t, initialBalance/2, res.Balance, "sender account balance mismatch")
			} else {
				// receiver
				require.Equal(t, initialBalance+initialBalance/2, res.Balance, "receiver account balance mismatch")
			}
			totalBalance += res.Balance
			return nil
		})
	require.NoError(t, err, "Failed to stream data for total balance validation")
	require.Equal(t, numAccounts*initialBalance, totalBalance, "Total balance mismatch")

	// Validate point access
	for _, a := range data {
		var account Ledger
		err := knox.NewGenericQuery[Ledger]().
			WithTable(table).
			AndEqual("id", a.Id).
			Execute(ctx, &account)
		require.NoError(t, err, "Failed to load account with ID: %d", a.Id)
		require.Equal(t, a.Id, account.Id, "Account id mismatch")
		if a.Id%2 == 1 {
			require.Equal(t, initialBalance/2, account.Balance, "sender account balance mismatch")
		} else {
			require.Equal(t, initialBalance+initialBalance/2, account.Balance, "receiver account balance mismatch")
		}
	}
}
