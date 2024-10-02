// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import "context"

type TransactionKey struct{}

func (e *Engine) WithTransaction(ctx context.Context, flags ...TxFlags) (context.Context, func() error, func() error) {
	// prevent duplicates, return noops because an outer call frame controls
	if tx := GetTransaction(ctx); tx != nil {
		return ctx, tx.Noop, tx.Noop
	}
	tx := e.NewTransaction(flags...)
	ctx = context.WithValue(ctx, TransactionKey{}, tx)
	return ctx, wrap(ctx, tx.Commit), wrap(ctx, tx.Abort)
}

func wrap(ctx context.Context, fn func(context.Context) error) func() error {
	return func() error {
		return fn(ctx)
	}
}

func GetTransaction(ctx context.Context) *Tx {
	val := ctx.Value(TransactionKey{})
	if val == nil {
		return nil
	}
	return val.(*Tx)
}
