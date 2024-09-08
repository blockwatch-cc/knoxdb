// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import "context"

type TransactionKey struct{}

func (e *Engine) WithTransaction(ctx context.Context) (context.Context, func() error, func() error) {
	// prevent duplicates
	if tx := GetTransaction(ctx); tx != nil {
		return ctx, tx.Commit, tx.Abort
	}
	tx := e.NewTransaction()
	ctx = context.WithValue(ctx, TransactionKey{}, tx)
	return ctx, tx.Commit, tx.Abort
}

func GetTransaction(ctx context.Context) *Tx {
	val := ctx.Value(TransactionKey{})
	if val == nil {
		return nil
	}
	return val.(*Tx)
}
