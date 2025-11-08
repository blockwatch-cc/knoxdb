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

func (t *Task) IsDone() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

func (t *Task) Done() <-chan struct{} {
	return t.done
}

func (t *Task) Err() error {
	return t.err
}

func (t *Task) Abort() {
	if t.err == nil {
		t.err = ErrTaskAborted
	}
}

func (t *Task) complete(err error) {
	t.err = err
	close(t.done)
}

// Worker executes tasks in a separate goroutine.
type Worker struct {
	job chan *Task
}

func NewWorker() *Worker {
	return &Worker{
		job: make(chan *Task, 1),
	}
}

func (w *Worker) Run(svc *TaskService) {
	defer svc.wg.Done()
	for {
		// make worker available for scheduling new task
		select {
		case <-svc.stop:
			return
		case svc.workers <- w:
		}

		// wait for new task or shutdown
		select {
		case <-svc.stop:
			return
		case job := <-w.job:
			job.complete(job.run(svc.ctx))
		}
	}
}

func (w *Worker) Close() {
	close(w.job)
	w.job = nil
}

type TaskService struct {
	ctx        context.Context
	cancel     context.CancelFunc
	tasks      chan *Task
	workers    chan *Worker
	stop       chan struct{}
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
		m.stop = make(chan struct{}, 1)
		m.log.Debugf("starting task service with queue=%d workers=%d", m.maxQueue, m.maxWorkers)
		for range m.maxWorkers {
			w := NewWorker()
			m.wg.Add(1)
			go w.Run(m)
		}
		m.wg.Add(1)
		go m.dispatch()
	})
}

func (m *TaskService) Stop() {
	m.log.Debugf("stopping task service")

	// signal shutdown to dispatcher and workers
	close(m.stop)

	// cancel context to stop running tasks early
	m.cancel()

	// wait for dispatcher and worker goroutines to exit
	m.wg.Wait()

	// finalize pending tasks
	m.drain()

	// close channels
	close(m.tasks)
	close(m.workers)
}

func (m *TaskService) Kill() {
	m.log.Debugf("killing task service")
	close(m.stop)
	m.cancel()
	m.wg.Wait()
	m.drain()
}

func (m *TaskService) dispatch() {
	defer m.wg.Done()
	for {
		// wait for the next task or shutdown
		var task *Task
		select {
		case <-m.stop:
			return
		case task = <-m.tasks:
		}

		// ignore aborted tasks
		if task.err != nil {
			continue
		}

		// wait for the next ready worker or shutdown
		select {
		case <-m.stop:
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
		case task := <-m.tasks:
			task.complete(ErrDatabaseShutdown)
		default:
			return
		}
	}
}
