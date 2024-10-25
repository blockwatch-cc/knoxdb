// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import "context"

type TransactionKey struct{}

func (e *Engine) WithTransaction(ctx context.Context, flags ...TxFlags) (context.Context, *Tx, func() error, func() error, error) {
	// prevent duplicates, return noops because an outer call frame controls
	if tx := GetTransaction(ctx); tx != nil {
		// check compatibility
		if tx.IsReadOnly() && !mergeFlags(flags).IsReadOnly() {
			return ctx, tx, noop, noop, ErrTxReadonly
		}

		// allow catalog flag update
		for _, f := range flags {
			if f == TxFlagsCatalog {
				tx.WithFlags(f)
			}
		}

		return ctx, tx, noop, noop, nil
	}

	// create new tx
	tx := e.NewTransaction(flags...)

	// check engine shutdown state
	if sd := e.shutdown.Load(); sd != nil && sd.(bool) {
		return ctx, tx, noop, noop, ErrDatabaseShutdown
	}

	// link tx to context
	ctx = context.WithValue(ctx, TransactionKey{}, tx)

	// use ctx in tx (will make cancelable and forward to commit/abort callbacks)
	tx.WithContext(ctx)

	return ctx, tx, tx.Commit, tx.Abort, nil
}

func noop() error {
	return nil
}

func GetTransaction(ctx context.Context) *Tx {
	val := ctx.Value(TransactionKey{})
	if val == nil {
		return nil
	}
	return val.(*Tx)
}

func GetTxId(ctx context.Context) uint64 {
	val := ctx.Value(TransactionKey{})
	if val == nil {
		return 0
	}
	return val.(*Tx).id
}
