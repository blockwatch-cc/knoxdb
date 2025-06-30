// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"sync"

	"github.com/echa/log"
)

type Task struct {
	done chan struct{}
	run  func(context.Context) error
	err  error
}

func NewTask(fn func(context.Context) error) *Task {
	return &Task{
		done: make(chan struct{}),
		run:  fn,
	}
}

func (t *Task) Done() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

func (t *Task) Err() error {
	return t.err
}

func (t *Task) complete(err error) {
	t.err = err
	close(t.done)
}

// Worker executes tasks in a separate goroutine.
type Worker struct {
	ctx  context.Context
	pool chan *Worker
	job  chan *Task
}

func NewWorker(ctx context.Context, pool chan *Worker) *Worker {
	return &Worker{
		ctx:  ctx,
		pool: pool,
		job:  make(chan *Task),
	}
}

func (w *Worker) Run() {
	for {
		// make worker available for scheduling new task
		w.pool <- w

		// wait for new task or shutdown
		select {
		case <-w.ctx.Done():
			return
		case job := <-w.job:
			job.complete(job.run(w.ctx))
		}
	}
}

func (w *Worker) Close() {
	w.pool = nil
	close(w.job)
}

type TaskService struct {
	ctx        context.Context
	cancel     context.CancelFunc
	tasks      chan *Task
	workers    chan *Worker
	maxWorkers int
	maxQueue   int
	log        log.Logger
	once       sync.Once
	wg         sync.WaitGroup
}

func NewTaskService() *TaskService {
	ctx, cancel := context.WithCancel(context.Background())
	return &TaskService{
		ctx:    ctx,
		cancel: cancel,
		log:    log.Disabled,
	}
}

func (m *TaskService) WithContext(ctx context.Context) *TaskService {
	m.cancel()
	m.ctx, m.cancel = context.WithCancel(ctx)
	return m
}

func (m *TaskService) WithLogger(l log.Logger) *TaskService {
	m.log = l
	return m
}

func (m *TaskService) WithLimits(maxWorkers, maxQueue int) *TaskService {
	m.maxWorkers = maxWorkers
	m.maxQueue = maxQueue
	return m
}

func (m *TaskService) Submit(t *Task) bool {
	select {
	case m.tasks <- t:
		return true
	default:
		return false
	}
}

func (m *TaskService) Start() {
	m.once.Do(func() {
		m.tasks = make(chan *Task, m.maxQueue)
		m.workers = make(chan *Worker, m.maxWorkers)
		m.log.Debugf("starting task service with queue=%d workers=%d", m.maxQueue, m.maxWorkers)
		for range m.maxWorkers {
			w := NewWorker(m.ctx, m.workers)
			go w.Run()
		}
		m.wg.Add(1)
		go m.dispatch()
	})
}

func (m *TaskService) Stop() {
	m.log.Debugf("stopping task service")

	// signal shutdown to all running goroutines
	m.cancel()

	// wait for dispatcher goroutine to exit
	m.wg.Wait()

	// wait for all workers to exit and free resources
	for range m.maxWorkers {
		w := <-m.workers
		w.Close()
	}

	// finalize pending tasks
	m.drain()

	// close channels
	if m.tasks != nil {
		close(m.tasks)
		m.tasks = nil
	}
	if m.workers != nil {
		close(m.workers)
		m.workers = nil
	}
}

func (m *TaskService) Kill() {
	m.log.Debugf("killing task service")
	m.cancel()
}

func (m *TaskService) dispatch() {
	defer m.wg.Done()
	for {
		// wait for the next task
		var task *Task
		select {
		case <-m.ctx.Done():
			return
		case task = <-m.tasks:
		}

		// pick next ready worker
		select {
		case <-m.ctx.Done():
			task.complete(ErrDatabaseShutdown)
			return
		case w := <-m.workers:
			w.job <- task
		}
	}
}

func (m *TaskService) drain() {
	for {
		select {
		case task, ok := <-m.tasks:
			if ok {
				task.complete(ErrDatabaseShutdown)
			}
		default:
			return
		}
	}
}
