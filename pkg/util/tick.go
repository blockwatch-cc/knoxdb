// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"sync"
	"time"
)

// Example
//
// const format = "15:04:05.999999999Z"
//
// func main() {
// 	t := util.NewAlignedTicker(5 * time.Second)
// 	fmt.Printf("Start %s\n", time.Now().UTC().Format(format))
// 	for i := 0; i < 5; i++ {
// 		select {
// 		case now := <-t.C:
// 			fmt.Printf("Tick %s\n", now.Format(format))
// 		}
// 	}
// 	t.Stop()
// }

type AlignedTicker struct {
	sync.Mutex
	sync.Once
	t *time.Ticker
	C <-chan time.Time
	c chan time.Time
}

func NewAlignedTicker(d time.Duration) *AlignedTicker {
	now := time.Now().UTC()
	wait := now.Add(d + time.Second).Truncate(d).Sub(now)
	c := make(chan time.Time, 1)
	t := &AlignedTicker{
		C: c,
		c: c,
	}
	go func() {
		now := <-time.After(wait)
		// prevent panic on early Stop (i.e. before wait is over)
		select {
		case c <- now:
		default:
			return
		}
		t.Lock()
		defer t.Unlock()
		t.t = time.NewTicker(d)
		t.C = t.t.C
	}()
	return t
}

func (t *AlignedTicker) Stop() {
	// run only once
	t.Do(func() {
		t.Lock()
		defer t.Unlock()
		if t.t != nil {
			t.t.Stop()
		}
		close(t.c)
	})
}

// More accurate Ticker from
// https://github.com/golang/go/issues/19810
type WallTicker struct {
	sync.Mutex
	sync.Once
	C      <-chan time.Time
	align  time.Duration
	offset time.Duration
	stop   chan bool
	c      chan time.Time
	skew   float64
	d      time.Duration
	last   time.Time
	tm     *time.Timer
}

func NewWallTicker(align, offset time.Duration) *WallTicker {
	w := &WallTicker{
		align:  align,
		offset: offset,
		stop:   make(chan bool),
		c:      make(chan time.Time, 1),
		skew:   1.0,
	}
	w.C = w.c
	w.start()
	return w
}

func (w *WallTicker) Stop() {
	// run only once
	w.Do(func() {
		close(w.stop)
		w.Lock()
		defer w.Unlock()
		if w.tm != nil {
			w.tm.Stop()
			w.tm = nil
		}
	})
}

// const fakeAzure = false
func (w *WallTicker) start() {
	w.Lock()
	defer w.Unlock()
	now := time.Now()
	d := time.Until(now.Add(-w.offset).Add(w.align * 4 / 3).Truncate(w.align).Add(w.offset))
	d = time.Duration(float64(d) / w.skew)
	w.d = d
	w.last = now
	// if fakeAzure {
	// 	d = time.Duration(float64(d) * 99 / 101)
	// }
	w.tm = time.AfterFunc(d, w.tick)
}

func (w *WallTicker) tick() {
	const α = 0.7 // how much weight to give past history
	now := time.Now()
	if now.After(w.last) {
		w.skew = w.skew*α + (float64(now.Sub(w.last))/float64(w.d))*(1-α)
		select {
		case <-w.stop:
			return
		case w.c <- now:
			// ok
		default:
			// client not keeping up, drop tick
		}
	}
	w.start()
}
