// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"
	"fmt"

	"github.com/echa/log"
)

// TODO
// - MetaPipeline with dependency between pipelines (join, union)

type PipelineState int

const (
	PipelineStateIdle PipelineState = iota
	PipelineStateReady
	PipelineStateBlocked
	PipelineStateDone
	PipelineStateError
)

type PhysicalPipeline struct {
	src  PullOperator
	ops  []PushOperator
	sink PushOperator
	// next  int
	state PipelineState
	log   log.Logger
	err   error
}

func NewPhysicalPipeline() *PhysicalPipeline {
	return &PhysicalPipeline{
		log: log.Disabled,
	}
}

func (p *PhysicalPipeline) Close() {
	if p.src != nil {
		p.src.Close()
		p.src = nil
	}
	if p.sink != nil {
		p.sink.Close()
		p.sink = nil
	}
	for _, op := range p.ops {
		op.Close()
	}
	clear(p.ops)
	p.ops = nil
	// p.next = 0
	p.state = 0
	p.log = nil
	p.err = nil
}

func (p *PhysicalPipeline) WithSource(src PullOperator) *PhysicalPipeline {
	p.src = src
	return p
}

func (p *PhysicalPipeline) WithOperator(op PushOperator) *PhysicalPipeline {
	p.ops = append(p.ops, op)
	return p
}

func (p *PhysicalPipeline) WithSink(sink PushOperator) *PhysicalPipeline {
	p.sink = sink
	return p
}

func (p *PhysicalPipeline) WithLogger(l log.Logger) *PhysicalPipeline {
	p.log = l
	return p
}

func (p *PhysicalPipeline) IsRunnable() bool {
	return p.state == PipelineStateIdle || p.state == PipelineStateReady
}

func (p *PhysicalPipeline) IsDone() bool {
	return p.state == PipelineStateError || p.state == PipelineStateDone
}

func (p *PhysicalPipeline) Validate() error {
	if p.src != nil {
		return ErrNoSource
	}
	if p.sink != nil {
		return ErrNoSink
	}
	return nil
}

// Executes a simple linear pipeline. TODO
// - pack recycling (operators may reuse the same pack or clone/copy/alloc a new, how to know?)
// - blocking operator with retry (ResultAgain), PipelineBlocked state and retry
func (p *PhysicalPipeline) Execute(ctx context.Context) error {
	if p.IsDone() {
		return p.err
	}

	// pull next pack from source
	pkg, res := p.src.Next(ctx)
	switch res {
	case ResultError:
		return p.fail(p.src.Err())
	case ResultDone:
		// TODO: allow pack & done, finalize later
		return p.finalize(ctx)
	case ResultOK:
		if pkg == nil {
			return ErrNilPack
		}
	case ResultMore:
		return fmt.Errorf("unexpected source result 'more'")
	}

	// process package assuming every operator returns either an error
	// or a package which can either be the same or a new one
	for _, op := range p.ops {
		pkg, res = op.Process(ctx, pkg)
		switch res {
		case ResultError:
			return p.fail(op.Err())
		case ResultMore:
			// operator needs more data to output a pack
			return nil
		case ResultDone:
			// any operator can signal we're done (have reached a limit)
			// so we stop the pipeline here. Done must not be accompanied
			// by a pack. TODO: allow pack & done
			return p.finalize(ctx)
		case ResultOK:
			if pkg == nil {
				return ErrNilPack
			}
		}
	}

	// push to sink, can return more
	_, res = p.sink.Process(ctx, pkg)
	switch res {
	case ResultError:
		return p.fail(p.sink.Err())
	case ResultMore:
		// expected
	case ResultDone:
		// expected (on limit), so finalize
		return p.finalize(ctx)
	case ResultOK:
		// expected
	}

	return nil
}

func (p *PhysicalPipeline) fail(err error) error {
	p.state = PipelineStateError
	p.err = err
	return err
}

func (p *PhysicalPipeline) finalize(ctx context.Context) error {
	if err := p.sink.Finalize(ctx); err != nil {
		return p.fail(p.sink.Err())
	}
	p.state = PipelineStateDone
	return nil
}
