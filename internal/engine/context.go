// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"

	"blockwatch.cc/knoxdb/internal/types"
)

type EngineKey struct{}

func WithEngine(ctx context.Context, e *Engine) context.Context {
	return context.WithValue(ctx, EngineKey{}, e)
}

func GetEngine(ctx context.Context) *Engine {
	val := ctx.Value(EngineKey{})
	if val == nil {
		return nil
	}
	return val.(*Engine)
}

type TransactionKey struct{}

func WithTx(ctx context.Context, tx *Tx) context.Context {
	return context.WithValue(ctx, TransactionKey{}, tx)
}

func GetTx(ctx context.Context) *Tx {
	val := ctx.Value(TransactionKey{})
	if val == nil {
		return nil
	}
	return val.(*Tx)
}

func GetTxId(ctx context.Context) types.XID {
	val := ctx.Value(TransactionKey{})
	if val == nil {
		return 0
	}
	return val.(*Tx).id
}

func GetSnapshot(ctx context.Context) *types.Snapshot {
	val := ctx.Value(TransactionKey{})
	if val == nil {
		return nil
	}
	return val.(*Tx).Snapshot()
}
