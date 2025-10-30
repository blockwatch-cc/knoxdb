// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"time"

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

func (e *Engine) WithTransaction(ctx context.Context, flags ...TxFlags) (context.Context, *Tx, func() error, func() error, error) {
	// merge flags
	uflags := mergeFlags(flags)

	// prevent duplicates, return noops because an outer call frame controls
	if tx := GetTx(ctx); tx != nil {
		// check compatibility
		if tx.IsReadOnly() && !uflags.IsReadOnly() {
			return ctx, tx, noop, noop, ErrTxReadonly
		}

		// allow catalog flag update
		for _, f := range flags {
			if f == TxFlagCatalog {
				tx.WithFlags(f)
			}
		}

		return ctx, tx, noop, noop, nil
	}

	// check readonly state
	if e.IsReadOnly() && !uflags.IsReadOnly() {
		return ctx, nil, noop, noop, ErrDatabaseReadOnly
	}

	// check engine shutdown state
	if e.IsShutdown() {
		return ctx, nil, noop, noop, ErrDatabaseShutdown
	}

	// potentially wait
	ok := true
	if uflags.IsReadOnly() {
		// enforce deferred flag
		if uflags.IsDeferred() {
			switch {
			case e.opts.TxWaitTimeout > 0:
				select {
				case _, ok = <-e.txchan:
				case <-time.After(e.opts.TxWaitTimeout):
					return ctx, nil, noop, noop, ErrTxTimeout
				}
			default:
				_, ok = <-e.txchan
			}
		}
	} else {
		// enforce single writer tx
		switch {
		case uflags.IsNoWait():
			select {
			case _, ok = <-e.txchan:
			default:
				return ctx, nil, noop, noop, ErrTxConflict
			}
		case e.opts.TxWaitTimeout > 0:
			select {
			case _, ok = <-e.txchan:
			case <-time.After(e.opts.TxWaitTimeout):
				return ctx, nil, noop, noop, ErrTxTimeout
			}
		default:
			_, ok = <-e.txchan
		}
	}

	// channel was closed during wait
	if !ok {
		return ctx, nil, noop, noop, ErrDatabaseShutdown
	}

	// create new tx
	tx := e.NewTransaction(uflags)

	// return writer token after deferred reader wait
	if uflags.IsDeferred() {
		e.txchan <- struct{}{}
	}

	// link tx and engine to context and derive tx context
	// (will make context cancelable via tx, forwards to commit/abort callbacks)
	tx.WithContext(WithTx(WithEngine(ctx, e), tx))

	return tx.ctx, tx, tx.Commit, tx.Abort, nil
}

func noop() error {
	return nil
}
