// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"fmt"
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/internal/types"
)

// Base sizes
//
// n: length, c: cardinality, len(n): length of elem n
//
// Algo     head      meta  dict      raw data       32k example
// --------------------------------------------------------------------
// Native   24        24n             ∑_1_n(len(n))  768k + n*len(n)
// Compact  3*24      8n              ∑_1_c(len(n))  256k + c*len(n)
// Fixed    8+24                      ∑_1_n(len(n))    32 + n*len(n)
// Dict     4*24+16   8c   n*log2(c)  ∑_1_c(len(n))   112 + 8c + n*log2(c) + c*len(n)
//
//
// Examples
//          max unique   dict cutoff  op_hash     op_store (avg)     <- Use Case
// Algo     32k/32       16k/32       22k/32      2500/66            <- c/len(n)
// ---------------------------------------------------------------
// Native   1.75M       1.75M        1.75M        2.88M
// Compact  1.25M -28%   768k -56%    943k -46%    417k -86%
// Fixed       1M -42%     1M -42%    1M   -42%    2.1M -27%
// Dict     1.34M -23%   696k -60%    906k -48%    229k -92%
//
// Algo selection
//
// Dataset                   Best Algo
// ----------------------------------------
// all zeros              -> fixed (e.g. all-empty params)
// fixed len + no zeros   -> fixed
// dyn len + card < n/2   -> dict
// dyn len + card >= n/2  -> compact (e.g. block/op hashes)

type StringContainer interface {
	// introspect
	Type() ContainerType // returns encoding type
	Info() string        // describes encoding and nested containers

	// encode and I/O
	Encode(ctx *StringContext, vals types.StringAccessor) StringContainer
	Store([]byte) []byte         // serializes into buf, returns updated buf
	Load([]byte) ([]byte, error) // deserializes from buf, returns updated buf

	// Common string vector access interface
	types.StringAccessor
	types.StringMatcher
}

type StringContext struct {
	Min        []byte         // vector minimum
	Max        []byte         // vector maximum
	MinLen     int            // min string length
	MaxLen     int            // max string length
	NumUnique  int            // vector cardinality (hint, may not be precise)
	NumValues  int            // vector length
	UniqueSize int            // size of unique strings in bytes
	UniqueMap  map[uint64]int // unique values hash map to id (optional)
	Dups       []int32        // <0 = unique string, >=0 position of original
}

func (c *StringContext) Close() {
	if c.UniqueMap != nil {
		clear(c.UniqueMap)
	}
	if c.Dups != nil {
		arena.Free(c.Dups)
		c.Dups = nil
	}
	c.Min = nil
	c.Max = nil
	c.MinLen = 0
	c.MaxLen = 0
	c.NumUnique = 0
	c.NumValues = 0
	c.UniqueSize = 0
	putStringContext(c)
}

func (c *StringContext) MinMax() (any, any) {
	return bytes.Clone(c.Min), bytes.Clone(c.Max)
}

func (c *StringContext) Unique() int {
	return c.NumUnique
}

var emptyHash uint64 = 11400714785074694791 // xxhash64 prime1 used as AES hash seed

// AnalyzeString produces statistics about []byte vectors.
func AnalyzeString(vals types.StringAccessor) *StringContext {
	c := newStringContext()
	c.NumValues = vals.Len()
	if c.UniqueMap == nil {
		c.UniqueMap = make(map[uint64]int, c.NumValues)
	}
	if cap(c.Dups) < c.NumValues {
		arena.Free(c.Dups)
		c.Dups = arena.Alloc[int32](c.NumValues)[:c.NumValues]
	}

	// analyze
	if vals.Len() > 0 {
		c.Min = vals.Get(0)
		c.Max = c.Min
		c.MinLen = len(c.Min)
		c.MaxLen = c.MinLen
		for i, v := range vals.Iterator() {
			if bytes.Compare(v, c.Min) < 0 {
				c.Min = v
			} else if bytes.Compare(v, c.Max) > 0 {
				c.Max = v
			}
			vlen := len(v)
			c.MinLen = min(c.MinLen, vlen)
			c.MaxLen = max(c.MaxLen, vlen)
			h := emptyHash
			if vlen > 0 {
				h = hash.MemHash(v, emptyHash)
			}
			if j, ok := c.UniqueMap[h]; ok {
				c.Dups[i] = int32(j)
			} else {
				c.UniqueMap[h] = c.NumUnique
				c.Dups[i] = -1
				c.NumUnique++
				c.UniqueSize += vlen
			}
		}
	}
	return c
}

func (c *StringContext) UseScheme() ContainerType {
	switch {
	case c.MaxLen == 0 || bytes.Equal(c.Min, c.Max):
		// use const when all strings are empty or equal
		return TStringConstant
	case c.MinLen == c.MaxLen && c.NumUnique == c.NumValues:
		// prefer fixed when all values are different and fixed size
		return TStringFixed
	case c.NumUnique < c.NumValues/2:
		// prefer dict when at least half the values are duplicates
		return TStringDictionary
	default:
		// use compact otherwise (it also handles duplicates but less efficient)
		return TStringCompact
	}
}

func newStringContext() *StringContext {
	return stringContextFactory.Get().(*StringContext)
}

func putStringContext(c *StringContext) {
	stringContextFactory.Put(c)
}

var stringContextFactory = sync.Pool{
	New: func() any { return new(StringContext) },
}

// NewString creates a new integer container from scheme type.
func NewString(typ ContainerType) StringContainer {
	switch typ {
	case TStringConstant:
		return newStringContainer[ConstStringContainer](typ)
	case TStringFixed:
		return newStringContainer[FixedStringContainer](typ)
	case TStringCompact:
		return newStringContainer[CompactStringContainer](typ)
	case TStringDictionary:
		return newStringContainer[DictStringContainer](typ)
	default:
		panic(fmt.Errorf("invalid string scheme %d (%s)", typ, typ))
	}
}

// EncodeString encodes a string vector ([]byte) into a container
// selecting the most efficient encoding scheme.
func EncodeString(ctx *StringContext, v types.StringAccessor) StringContainer {
	// analyze full data if missing
	if ctx == nil {
		ctx = AnalyzeString(v)
		defer ctx.Close()
	}

	// alloc best container and encode
	return NewString(ctx.UseScheme()).Encode(ctx, v)
}

// LoadString loads a string container from buffer.
func LoadString(buf []byte) (StringContainer, error) {
	c := NewString(ContainerType(buf[0]))
	if _, err := c.Load(buf); err != nil {
		return nil, err
	}
	return c, nil
}

type StringFactory struct {
	constPool     sync.Pool // containers
	fixedPool     sync.Pool
	compactPool   sync.Pool
	dictPool      sync.Pool
	fixedItPool   sync.Pool // iterators
	compactItPool sync.Pool
	dictItPool    sync.Pool
}

func newStringContainer[T any](typ ContainerType) *T {
	switch typ {
	case TStringConstant:
		return stringFactory.constPool.Get().(*T)
	case TStringFixed:
		return stringFactory.fixedPool.Get().(*T)
	case TStringCompact:
		return stringFactory.compactPool.Get().(*T)
	case TStringDictionary:
		return stringFactory.dictPool.Get().(*T)
	default:
		return nil
	}
}

func putStringContainer[T StringContainer](c T) {
	switch c.Type() {
	case TStringConstant:
		stringFactory.constPool.Put(c)
	case TStringFixed:
		stringFactory.fixedPool.Put(c)
	case TStringCompact:
		stringFactory.compactPool.Put(c)
	case TStringDictionary:
		stringFactory.dictPool.Put(c)
	}
}

func newStringIterator[T any](typ ContainerType) *T {
	switch typ {
	case TStringFixed:
		return stringFactory.fixedItPool.Get().(*T)
	case TStringCompact:
		return stringFactory.compactItPool.Get().(*T)
	case TStringDictionary:
		return stringFactory.dictItPool.Get().(*T)
	default:
		return nil
	}
}

func putStringIterator[T types.StringIterator](c T) {
	switch any(c).(type) {
	case *FixedStringIterator:
		stringFactory.fixedItPool.Put(c)
	case *CompactStringIterator:
		stringFactory.compactItPool.Put(c)
	case *DictStringIterator:
		stringFactory.dictItPool.Put(c)
	}
}

var stringFactory = StringFactory{
	constPool: sync.Pool{
		New: func() any { return new(ConstStringContainer) },
	},
	fixedPool: sync.Pool{
		New: func() any { return new(FixedStringContainer) },
	},
	compactPool: sync.Pool{
		New: func() any { return new(CompactStringContainer) },
	},
	dictPool: sync.Pool{
		New: func() any { return new(DictStringContainer) },
	},
	fixedItPool: sync.Pool{
		New: func() any { return new(FixedStringIterator) },
	},
	compactItPool: sync.Pool{
		New: func() any { return new(CompactStringIterator) },
	},
	dictItPool: sync.Pool{
		New: func() any { return new(DictStringIterator) },
	},
}
