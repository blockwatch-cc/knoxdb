// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, stefan@blockwatch.cc

package rclru

import (
    "math/rand"
    "testing"

    "blockwatch.cc/knoxdb/encoding/block"
)

func NewTest2Q(sz int) (*TwoQueueCache[string, *TestPackage], error) {
    return New2Q[string, *TestPackage](sz)
}

func Benchmark2Q_Rand(b *testing.B) {
    l, err := NewTest2Q(8192 * szPackage)
    if err != nil {
        b.Fatalf("err: %v", err)
    }

    trace := make([]uint32, b.N*2)
    for i := 0; i < b.N*2; i++ {
        trace[i] = rand.Uint32() % 32768
    }

    b.ResetTimer()

    var hit, miss int
    for i := 0; i < 2*b.N; i++ {
        if i%2 == 0 {
            pkg := NewTestPackage(trace[i], 0)
            l.Add(pkg.Key(), pkg)
        } else {
            _, ok := l.Get(encodeKeyU32(trace[i]))
            if ok {
                hit++
            } else {
                miss++
            }
        }
    }
    b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(miss))
}

func Benchmark2Q_Freq(b *testing.B) {
    l, err := NewTest2Q(8192 * szPackage)
    if err != nil {
        b.Fatalf("err: %v", err)
    }

    trace := make([]uint32, b.N*2)
    for i := 0; i < b.N*2; i++ {
        if i%2 == 0 {
            trace[i] = rand.Uint32() % 16384
        } else {
            trace[i] = rand.Uint32() % 32768
        }
    }

    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        pkg := NewTestPackage(trace[i], 0)
        l.Add(pkg.Key(), pkg)
    }
    var hit, miss int
    for i := 0; i < b.N; i++ {
        _, ok := l.Get(encodeKeyU32(trace[i]))
        if ok {
            hit++
        } else {
            miss++
        }
    }
    b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(miss))
}

func Test2Q_RandomOps(t *testing.T) {
    size := 128 * (szPackage + block.BlockSz + 16384)
    l, err := NewTest2Q(size)
    if err != nil {
        t.Fatalf("err: %v", err)
    }

    n := 200000
    for i := 0; i < n; i++ {
        key := uint32(rand.Int63() % 512)
        r := rand.Int63()
        switch r % 3 {
        case 0:
            size := rand.Intn(32768)
            pkg := NewTestPackage(key, size)
            b := block.NewBlock(block.BlockUint8, 0, size)
            b.Uint8 = b.Uint8[:size]
            pkg.blocks = append(pkg.blocks, b)
            l.Add(pkg.Key(), pkg)
            if int(pkg.refCount) != 1 {
                t.Fatalf("bad: refCount == %d after Add", pkg.refCount)
            }

        case 1:
            if cached, ok := l.Get(encodeKeyU32(key)); ok {
                pkg := cached
                if int(pkg.refCount) != 2 {
                    t.Fatalf("bad: refCount == %d after Get", pkg.refCount)
                }
                pkg.DecRef()
            }
        case 2:
            l.Remove(encodeKeyU32(key))
        }

        if l.Size() > size {
            t.Fatalf("bad: byteSize: %d size: %d recent: %d freq: %d",
                l.Size(), size, l.recent.Len(), l.frequent.Len())
        }
    }
}

func Test2Q_Get_RecentToFrequent(t *testing.T) {
    l, err := NewTest2Q(128 * szPackage)
    if err != nil {
        t.Fatalf("err: %v", err)
    }

    // Touch all the entries, should be in t1
    for i := uint32(0); i < 128; i++ {
        pkg := NewTestPackage(i, 0)
        l.Add(pkg.Key(), pkg)
    }
    if n := l.recent.Len(); n != 128 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 0 {
        t.Fatalf("bad: %d", n)
    }

    // Get should upgrade to t2
    for i := uint32(0); i < 128; i++ {
        _, ok := l.Get(encodeKeyU32(i))
        if !ok {
            t.Fatalf("missing: %d", i)
        }
    }
    if n := l.recent.Len(); n != 0 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 128 {
        t.Fatalf("bad: %d", n)
    }

    // Get be from t2
    for i := uint32(0); i < 128; i++ {
        _, ok := l.Get(encodeKeyU32(i))
        if !ok {
            t.Fatalf("missing: %d", i)
        }
    }
    if n := l.recent.Len(); n != 0 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 128 {
        t.Fatalf("bad: %d", n)
    }
}

func Test2Q_Add_RecentToFrequent(t *testing.T) {
    l, err := NewTest2Q(128 * szPackage)
    if err != nil {
        t.Fatalf("err: %v", err)
    }

    // Add initially to recent
    pkg := NewTestPackage(1, 0)
    l.Add(pkg.Key(), pkg)
    if n := l.recent.Len(); n != 1 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 0 {
        t.Fatalf("bad: %d", n)
    }

    // Add should upgrade to frequent
    pkg = NewTestPackage(1, 0)
    l.Add(pkg.Key(), pkg)
    if n := l.recent.Len(); n != 0 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 1 {
        t.Fatalf("bad: %d", n)
    }

    // Add should remain in frequent
    pkg = NewTestPackage(1, 0)
    l.Add(pkg.Key(), pkg)
    if n := l.recent.Len(); n != 0 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 1 {
        t.Fatalf("bad: %d", n)
    }
}

func Test2Q_Add_RecentEvict(t *testing.T) {
    l, err := NewTest2Q(4 * szPackage)
    if err != nil {
        t.Fatalf("err: %v", err)
    }

    // Add 1,2,3,4,5 -> Evict 1
    for i := uint32(1); i < 6; i++ {
        pkg := NewTestPackage(i, 0)
        l.Add(pkg.Key(), pkg)
    }
    if n := l.recent.Len(); n != 4 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.recentEvict.Len(); n != 1 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 0 {
        t.Fatalf("bad: %d", n)
    }

    // Pull in the recently evicted
    pkg := NewTestPackage(1, 0)
    l.Add(pkg.Key(), pkg)
    if n := l.recent.Len(); n != 3 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.recentEvict.Len(); n != 1 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 1 {
        t.Fatalf("bad: %d", n)
    }

    // Add 6, should cause another recent evict
    pkg = NewTestPackage(6, 0)
    l.Add(pkg.Key(), pkg)
    if n := l.recent.Len(); n != 3 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.recentEvict.Len(); n != 2 {
        t.Fatalf("bad: %d", n)
    }
    if n := l.frequent.Len(); n != 1 {
        t.Fatalf("bad: %d", n)
    }
}

func Test2Q(t *testing.T) {
    l, err := NewTest2Q(128 * szPackage)
    if err != nil {
        t.Fatalf("err: %v", err)
    }
    pkg := make([]*TestPackage, 256)
    for i := uint32(0); i < 256; i++ {
        pkg[i] = NewTestPackage(i, 0)
        l.Add(pkg[i].Key(), pkg[i])
    }
    if l.Len() != 128 {
        t.Fatalf("bad len: %v", l.Len())
    }

    for i, k := range l.Keys() {
        if v, ok := l.Get(k); !ok || v.Key() != k || int(v.key) != i+128 {
            t.Fatalf("bad key: %v", k)
        } else {
            if v.refCount != 2 {
                t.Errorf("refCount of %d should be 2: %v", v.key, v)
            }
            v.DecRef()
        }
    }
    for i := uint32(0); i < 128; i++ {
        _, ok := l.Get(encodeKeyU32(i))
        if ok {
            t.Fatalf("should be evicted")
        }
    }
    for i := uint32(128); i < 256; i++ {
        v, ok := l.Get(encodeKeyU32(i))
        if ok {
            if v.refCount != 2 {
                t.Errorf("refCount of %d should be 2: %v", v.key, v)
            }
            v.DecRef()
        } else {
            t.Fatalf("should not be evicted")
        }
    }
    for i := uint32(128); i < 192; i++ {
        l.Remove(encodeKeyU32(i))
        _, ok := l.Get(encodeKeyU32(i))
        if ok {
            t.Fatalf("should be deleted")
        }
    }

    l.Purge()
    if l.Len() != 0 {
        t.Fatalf("bad len: %v", l.Len())
    }
    if _, ok := l.Get(encodeKeyU32(200)); ok {
        t.Fatalf("should contain nothing")
    }
}

// Test that Contains doesn't update recent-ness
func Test2Q_Contains(t *testing.T) {
    l, err := NewTest2Q(2 * szPackage)
    if err != nil {
        t.Fatalf("err: %v", err)
    }

    for i := uint32(1); i < 3; i++ {
        pkg := NewTestPackage(i, 0)
        l.Add(pkg.Key(), pkg)
    }
    if !l.Contains(encodeKeyU32(1)) {
        t.Errorf("1 should be contained")
    }

    pkg := NewTestPackage(3, 0)
    l.Add(pkg.Key(), pkg)
    if l.Contains(encodeKeyU32(1)) {
        t.Errorf("Contains should not have updated recent-ness of 1")
    }
}

// Test that Peek doesn't update recent-ness
func Test2Q_Peek(t *testing.T) {
    l, err := NewTest2Q(2 * szPackage)
    if err != nil {
        t.Fatalf("err: %v", err)
    }

    for i := uint32(1); i < 3; i++ {
        pkg := NewTestPackage(i, 0)
        l.Add(pkg.Key(), pkg)
    }
    if v, ok := l.Peek("1"); !ok || v.key != 1 {
        t.Errorf("1 should be set to 1: %v, %v", v, ok)
    } else {
        if v.refCount != 2 {
            t.Errorf("refCount of 1 should be 2: %v", v)
        }
    }

    pkg := NewTestPackage(3, 0)
    l.Add(pkg.Key(), pkg)
    if l.Contains(encodeKeyU32(1)) {
        t.Errorf("should not have updated recent-ness of 1")
    }
}
