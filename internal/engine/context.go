// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import "context"

type TransactionKey struct{}

func (e *Engine) WithTransaction(ctx context.Context, flags ...TxFlags) (context.Context, func() error, func() error) {
	// prevent duplicates, return noops because an outer call frame controls
	if tx := GetTransaction(ctx); tx != nil {
		tx.WithFlags(flags...)
		return ctx, noop, noop
	}
	tx := e.NewTransaction(flags...)

	// check engine shutdown state and return a defunct transaction object
	if sd := e.shutdown.Load(); sd != nil && sd.(bool) {
		tx.kill()
	}

	// link tx to context
	ctx = context.WithValue(ctx, TransactionKey{}, tx)

	// use ctx in tx (will make cancelable and forward to commit/abort callbacks)
	tx.WithContext(ctx)

	return ctx, tx.Commit, tx.Abort
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
