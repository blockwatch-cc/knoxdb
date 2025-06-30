// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"
	"errors"

	"blockwatch.cc/knoxdb/internal/pack"
)

type Result byte

const (
	ResultOK Result = iota
	ResultMore
	ResultDone
	ResultError
)

var (
	ErrNoPull = errors.New("pull mode not supported")
	ErrNoPush = errors.New("push mode not supported")
	ErrClosed = errors.New("operator closed")
	ErrTodo   = errors.New("operator not implemented")
)

type PullOperator interface {
	Next(context.Context) (*pack.Package, Result)
	Err() error
	Close()
}

type PushOperator interface {
	Process(context.Context, *pack.Package) (*pack.Package, Result)
	Finalize(context.Context) (*pack.Package, Result)
	Err() error
	Close()
}

// type Operator interface {
// 	Next(context.Context) (*pack.Package, Result)
// 	More() bool
// 	Process(context.Context, *pack.Package) Result
// 	Finalize(context.Context) (*pack.Package, Result)
// 	Err() error
// 	Close()
// }
