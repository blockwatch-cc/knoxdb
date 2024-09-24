// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"sync"

	"github.com/echa/log"
)

type MergerClient interface {
	Merge(Context, []byte) error
}

type Task struct {
	Client MergerClient
	Data   []byte
}

type MergerService struct {
	ctx    context.Context
	cancel context.CancelFunc
	tasks  chan Task
	log    log.Logger
	once   sync.Once
	wg     sync.WaitGroup
}

func NewMergerService() *MergerService {
	ctx, cancel := context.WithCancel(context.Background())
	return &MergerService{
		ctx:    ctx,
		cancel: cancel,
		tasks:  make(chan Task, 0),
		log:    log.Disabled,
	}
}

func (m *MergerService) WithContext(ctx context.Context) *MergerService {
	m.cancel()
	m.ctx, m.cancel = context.WithCancel(ctx)
	return m
}

func (m *MergerService) WithLogger(l log.Logger) *MergerService {
	m.log = l
	return m
}

func (m *MergerService) Submit(t Task) {
	m.tasks <- t
}

func (m *MergerService) Start() {
	m.once.Do(func() {
		m.wg.Add(1)
		go m.loop()
	})
}

func (m *MergerService) Stop() {
	m.cancel()
	m.wg.Wait()
}

func (m *MergerService) loop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case task := <-m.tasks:
			err := task.Client.Merge(m.ctx, task.Data)
			if err != nil {
				m.log.Errorf("merge: %v", err)
			}
		}
	}
}
