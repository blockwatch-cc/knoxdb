// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import "context"

type Executor struct {
	pipelines []*PhysicalPipeline
}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) AddPipeline(p *PhysicalPipeline) *Executor {
	e.pipelines = append(e.pipelines, p)
	return e
}

func (e *Executor) Run(ctx context.Context) error {
	if len(e.pipelines) == 0 {
		return nil
	}

	var nActive = len(e.pipelines)

	// stop when all pipelines are done
	for nActive > 0 {
		// find the next runnable pipeline and execute it
		for _, p := range e.pipelines {
			if !p.IsRunnable() {
				continue
			}
			err := p.Execute(ctx)
			if err != nil {
				return err
			}
			if p.IsDone() {
				nActive--
			}
		}
	}

	return nil
}
