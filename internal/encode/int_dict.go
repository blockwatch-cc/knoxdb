// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"slices"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// TIntegerDictionary
type DictionaryContainer[T types.Integer] struct {
	For    T
	Values IntegerContainer[T]
	Codes  IntegerContainer[uint16]
}

func (c *DictionaryContainer[T]) Type() IntegerContainerType {
	return TIntegerDictionary
}

func (c *DictionaryContainer[T]) Len() int {
	return c.Codes.Len()
}

func (c *DictionaryContainer[T]) MaxSize() int {
	return 1 + num.MaxVarintLen64 + c.Values.MaxSize() + c.Codes.MaxSize()
}

func (c *DictionaryContainer[T]) Store(dst []byte) []byte {
	dst = append(dst, byte(TIntegerDictionary))
	dst = num.AppendUvarint(dst, uint64(c.For))
	dst = c.Values.Store(dst)
	return c.Codes.Store(dst)
}

func (c *DictionaryContainer[T]) Load(buf []byte) ([]byte, error) {
	if buf[0] != byte(TIntegerDictionary) {
		return buf, ErrInvalidType
	}
	buf = buf[1:]
	v, n := num.Uvarint(buf)
	c.For = T(v)
	buf = buf[n:]

	// alloc and decode values child container
	c.Values = NewInt[T](IntegerContainerType(buf[0]))
	var err error
	buf, err = c.Values.Load(buf)
	if err != nil {
		return buf, err
	}

	// alloc and decode ends child container
	c.Codes = NewInt[uint16](IntegerContainerType(buf[0]))
	return c.Codes.Load(buf)
}

func (c *DictionaryContainer[T]) Get(n int) T {
	return c.Values.Get(int(c.Codes.Get(n))) + c.For
}

func (c *DictionaryContainer[T]) AppendTo(sel []uint32, dst []T) []T {
	for _, v := range sel {
		dst = append(dst, c.Get(int(v)))
	}
	return dst
}

func (c *DictionaryContainer[T]) Encode(ctx *IntegerContext[T], vals []T, lvl int) IntegerContainer[T] {
	// init FOR
	c.For = ctx.Min

	// construct unique values map (if not done during analysis)
	unique := ctx.Unique
	if unique == nil {
		unique = make(map[T]uint16, ctx.NumUnique)
		for _, v := range vals {
			unique[v] = 0
		}
	}

	// construct dict from unique values (apply FOR)
	dict := make([]T, 0, ctx.NumUnique)
	for v := range unique {
		dict = append(dict, v-c.For)
	}

	// sort dict
	slices.Sort(dict)

	// remap dict codes to original values (we re-use the existing Unique map
	// to avoid more allocations)
	for i, v := range dict {
		unique[v+c.For] = uint16(i)
	}

	// construct codes
	codes := make([]uint16, len(vals))
	for i, v := range vals {
		codes[i] = unique[v]
	}

	// encode child containers
	// fmt.Println("Dict Values ..")
	c.Values = EncodeInt(nil, dict, lvl-1)
	// fmt.Println("Dict Codes ..")
	c.Codes = EncodeInt(nil, codes, lvl-1)
	// fmt.Println("Dict done.")
	return c
}

func (c *DictionaryContainer[T]) MatchEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchNotEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchLess(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchLessEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchGreater(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchBetween(a, b T, bits, mask *Bitset) *Bitset {
	return nil
}

func (c *DictionaryContainer[T]) MatchSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}

func (c *DictionaryContainer[T]) MatchNotSet(s any, bits, mask *Bitset) *Bitset {
	// set := s.(*xroar.Bitmap)
	return nil
}
