// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

// ring buffer https://www.snellman.net/blog/archive/2016-12-13-ring-buffers/
type RingBuffer[T any] struct {
	queue []T
	head  uint64
	tail  uint64
}

func NewRingBuffer[T any](cap int) *RingBuffer[T] {
	size := uint64(1)
	for size < uint64(cap) {
		size <<= 1
	}
	return &RingBuffer[T]{
		queue: make([]T, size),
	}
}

func (p *RingBuffer[T]) Len() int {
	return int(p.tail - p.head)
}

func (p *RingBuffer[T]) Cap() int {
	return len(p.queue)
}

func (p *RingBuffer[T]) IsFull() bool {
	return p.Len() == len(p.queue)
}

func (p *RingBuffer[T]) IsEmpty() bool {
	return p.head == p.tail
}

func (p *RingBuffer[T]) Pop() (t T) {
	if p.IsEmpty() {
		return
	}
	t = p.queue[p.mask(p.head)]
	p.head++
	return
}

func (p *RingBuffer[T]) Push(e T) bool {
	if p.IsFull() {
		return false
	}
	p.tail++
	p.queue[p.mask(p.tail)] = e
	return true
}

func (p *RingBuffer[T]) Clear() {
	clear(p.queue)
	p.head = 0
	p.tail = 0
}

func (p *RingBuffer[T]) mask(val uint64) uint64 {
	return val & uint64(len(p.queue)-1)
}

func (p *RingBuffer[T]) Processed() uint64 {
	return p.head
}

func (p *RingBuffer[T]) Consumed() uint64 {
	return p.tail
}
