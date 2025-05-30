// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"bytes"
	"fmt"
	"regexp"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"github.com/echa/log"
)

type BytesMatcherFactory struct{}

func (f BytesMatcherFactory) New(m FilterMode) Matcher {
	switch m {
	case FilterModeEqual:
		return &bytesEqualMatcher{}
	case FilterModeNotEqual:
		return &bytesNotEqualMatcher{}
	case FilterModeGt:
		return &bytesGtMatcher{}
	case FilterModeGe:
		return &bytesGeMatcher{}
	case FilterModeLt:
		return &bytesLtMatcher{}
	case FilterModeLe:
		return &bytesLeMatcher{}
	case FilterModeRange:
		return &bytesRangeMatcher{}
	case FilterModeIn:
		return &bytesInSetMatcher{}
	case FilterModeNotIn:
		return &bytesNotInSetMatcher{}
	case FilterModeRegexp:
		return &bytesRegexpMatcher{}
	default:
		return &noopMatcher{}
	}
}

type bytesMatcher struct {
	noopMatcher
	val  []byte
	hash filter.HashValue
}

func (m *bytesMatcher) Weight() int { return len(m.val) }

func (m *bytesMatcher) WithValue(v any) {
	m.val = v.([]byte)
	m.hash = filter.Hash(m.val)
}

func (m *bytesMatcher) Value() any {
	return m.val
}

func (m bytesMatcher) MatchFilter(flt filter.Filter) bool {
	return flt.Contains(m.hash.Uint64())
}

// EQUAL ---

type bytesEqualMatcher struct {
	bytesMatcher
}

func (m bytesEqualMatcher) MatchValue(v any) bool {
	return bytes.Equal(m.val, v.([]byte))
}

func (m bytesEqualMatcher) MatchRange(from, to any) bool {
	fromBytes, toBytes := from.([]byte), to.([]byte)
	if len(fromBytes) == 0 {
		return true
	}
	switch bytes.Compare(m.val, fromBytes) {
	case 0:
		return true
	case -1:
		return false
	}
	return bytes.Compare(m.val, toBytes) <= 0
}

func (m bytesEqualMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	b.Bytes().Matcher().MatchEqual(m.val, bits, mask)
}

func (m bytesEqualMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	// min <= v && max >= v, mask is optional
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.val)
	ge.WithValue(m.val)
	minBits := bitset.New(mins.Len())
	le.MatchVector(mins, minBits, mask)
	if mask != nil {
		minBits.And(mask)
	}
	ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
}

// NOT EQUAL ---

type bytesNotEqualMatcher struct {
	bytesMatcher
}

func (m bytesNotEqualMatcher) MatchValue(v any) bool {
	return !bytes.Equal(m.val, v.([]byte))
}

func (m bytesNotEqualMatcher) MatchRange(from, to any) bool {
	fromBytes, toBytes := from.([]byte), to.([]byte)
	if bytes.Compare(m.val, fromBytes) < 0 {
		return true
	}
	if bytes.Compare(m.val, toBytes) > 0 {
		return true
	}
	return false
}

func (m bytesNotEqualMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	b.Bytes().Matcher().MatchNotEqual(m.val, bits, mask)
}

func (m bytesNotEqualMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
}

// GT ---

type bytesGtMatcher struct {
	bytesMatcher
}

func (m bytesGtMatcher) MatchValue(v any) bool {
	return bytes.Compare(m.val, v.([]byte)) < 0
}

func (m bytesGtMatcher) MatchRange(_, to any) bool {
	return bytes.Compare(m.val, to.([]byte)) < 0
}

func (m bytesGtMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	b.Bytes().Matcher().MatchGreater(m.val, bits, mask)
}

func (m bytesGtMatcher) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) {
	// max > v
	gt := newFactory(maxs.Type()).New(FilterModeGt)
	gt.WithValue(m.val)
	gt.MatchVector(maxs, bits, mask)
}

// GE ---

type bytesGeMatcher struct {
	bytesMatcher
}

func (m bytesGeMatcher) MatchValue(v any) bool {
	return bytes.Compare(m.val, v.([]byte)) <= 0
}

func (m bytesGeMatcher) MatchRange(_, to any) bool {
	return bytes.Compare(m.val, to.([]byte)) <= 0
}

func (m bytesGeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	b.Bytes().Matcher().MatchGreaterEqual(m.val, bits, mask)
}

func (m bytesGeMatcher) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) {
	// max >= v
	ge := newFactory(maxs.Type()).New(FilterModeGe)
	ge.WithValue(m.val)
	ge.MatchVector(maxs, bits, mask)
}

// LT ---

type bytesLtMatcher struct {
	bytesMatcher
}

func (m bytesLtMatcher) MatchValue(v any) bool {
	return bytes.Compare(m.val, v.([]byte)) > 0
}

func (m bytesLtMatcher) MatchRange(from, _ any) bool {
	return bytes.Compare(m.val, from.([]byte)) > 0
}

func (m bytesLtMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	b.Bytes().Matcher().MatchLess(m.val, bits, mask)
}

func (m bytesLtMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) {
	// min < v
	lt := newFactory(mins.Type()).New(FilterModeLt)
	lt.WithValue(m.val)
	lt.MatchVector(mins, bits, mask)
}

// LE ---

type bytesLeMatcher struct {
	bytesMatcher
}

func (m bytesLeMatcher) MatchValue(v any) bool {
	return bytes.Compare(m.val, v.([]byte)) >= 0
}

func (m bytesLeMatcher) MatchRange(from, _ any) bool {
	return bytes.Compare(m.val, from.([]byte)) >= 0
}

func (m bytesLeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	b.Bytes().Matcher().MatchLessEqual(m.val, bits, mask)
}

func (m bytesLeMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) {
	// min <= v
	le := newFactory(mins.Type()).New(FilterModeLe)
	le.WithValue(m.val)
	le.MatchVector(mins, bits, mask)
}

// RANGE ---

type bytesRangeMatcher struct {
	noopMatcher
	from []byte
	to   []byte
}

func (m *bytesRangeMatcher) Weight() int { return len(m.from) + len(m.to) }

func (m *bytesRangeMatcher) Len() int { return 2 }

func (m *bytesRangeMatcher) WithValue(v any) {
	val := v.(RangeValue)
	m.from = val[0].([]byte)
	m.to = val[1].([]byte)
}

func (m *bytesRangeMatcher) Value() any {
	val := RangeValue{m.from, m.to}
	return val
}

func (m bytesRangeMatcher) MatchValue(v any) bool {
	return bytes.Compare(m.from, v.([]byte)) <= 0 && bytes.Compare(m.to, v.([]byte)) >= 0
}

func (m bytesRangeMatcher) MatchRange(from, to any) bool {
	return !(bytes.Compare(from.([]byte), m.to) > 0 || bytes.Compare(to.([]byte), m.from) < 0)
}

func (m bytesRangeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	b.Bytes().Matcher().MatchBetween(m.from, m.to, bits, mask)
}

func (m bytesRangeMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	// min <= to && max >= from
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.to)
	ge.WithValue(m.from)
	minBits := bitset.New(mins.Len())
	le.MatchVector(mins, minBits, mask)
	if mask != nil {
		minBits.And(mask)
	}
	ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
}

// Set matcher
//
// For fast string/byte comparisons we use a hash map to store query conditions
// arguments and consult this hash map for each element in a block. If the hash
// on an element does not match any of hash map entries we know for sure the element
// is not a match. False positives may exist in hash collision, so for any positive
// mash map lookup we also compare the element against the found condition.
// Hash collisions are handled with an overflow list that contains all duplicate
// hashes for the entire query condition list.

const filterThreshold = 2 // use hash map for IN conditions with at least N entries

type hashvalue struct {
	hash uint32 // value hash (colliding with another value hash)
	pos  int    // position in value list
}

type bytesSetMatcher struct {
	noopMatcher
	slice    *slicex.OrderedBytes // original query data, sorted, unique
	hashes   []filter.HashValue   // bloom hashes
	hmap     map[uint32]int       // compiled hashmap for quick byte/string set query lookup
	overflow []hashvalue          // hash collision overflow list
}

func (m *bytesSetMatcher) Weight() int { return 10 } // arbitrary cost for hash map access

func (m *bytesSetMatcher) Len() int { return m.slice.Len() }

func (m *bytesSetMatcher) Value() any {
	return m.slice.Values
}

func (m *bytesSetMatcher) WithValue(val any) {
	m.WithSlice(val)
}

func (m *bytesSetMatcher) WithSlice(slice any) {
	m.slice = slicex.NewOrderedBytes(slice.([][]byte)).SetUnique()
	m.hashes = filter.HashMulti(m.slice.Values)
	if len(m.slice.Values) > filterThreshold {
		// re-use bloom hash value [1] (xxhash32) as unique hash value
		m.hmap = make(map[uint32]int)
		for i, h := range m.hashes {
			val := m.slice.Values[i]
			if pos, ok := m.hmap[h[1]]; !ok {
				// no collision
				m.hmap[h[1]] = i
			} else {
				// handle collissions
				if pos != 0xFFFFFFFF {
					log.Warnf("knox: condition hash collision %0x / %0x == %0x", val, m.slice.Values[pos], h[1])
					m.overflow = append(m.overflow, hashvalue{
						hash: h[1],
						pos:  pos,
					})
				} else {
					log.Warnf("knox: condition double hash collision %0x == %0x", val, h[1])
				}
				m.overflow = append(m.overflow, hashvalue{
					hash: h[1],
					pos:  i,
				})
				m.hmap[h[1]] = 0xFFFFFFFF
			}
		}
	}
}

func (m bytesSetMatcher) matchHashMap(val []byte) bool {
	sum := xxhash.Sum32(val, 0)
	if pos, ok := m.hmap[sum]; ok {
		if pos != 0xFFFFFFFF {
			// compare slice value at pos to ensure we're collision free
			return bytes.Equal(val, m.slice.Values[pos])
		} else {
			// scan overflow list
			for _, v := range m.overflow {
				if v.hash != sum {
					continue
				}
				if !bytes.Equal(val, m.slice.Values[v.pos]) {
					continue
				}
				return true
			}
		}
	}
	return false
}

// IN ---

// In, Contains
type bytesInSetMatcher struct {
	bytesSetMatcher
}

func (m bytesInSetMatcher) MatchValue(v any) bool {
	return m.slice.Contains(v.([]byte))
}

func (m bytesInSetMatcher) MatchRange(from, to any) bool {
	return m.slice.ContainsRange(from.([]byte), to.([]byte))
}

func (m bytesInSetMatcher) MatchFilter(flt filter.Filter) bool {
	return flt.ContainsAny(m.hashes)
}

func (m bytesInSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if mask == nil {
		if m.hmap == nil {
			m.matchBlockSlice(b, bits)
		} else {
			m.matchBlockHashMap(b, bits)
		}
	} else {
		if m.hmap == nil {
			m.matchBlockSliceWithMask(b, bits, mask)
		} else {
			m.matchBlockHashMapWithMask(b, bits, mask)
		}
	}
}

func (m bytesInSetMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	setMin, setMax := m.slice.MinMax()
	rg := newFactory(mins.Type()).New(FilterModeRange)
	rg.WithValue(RangeValue{setMin, setMax})
	rg.MatchRangeVectors(mins, maxs, bits, mask)
}

func (m bytesInSetMatcher) matchBlockHashMap(b *block.Block, bits *bitset.Bitset) {
	for i, v := range b.Bytes().Iterator() {
		if m.matchHashMap(v) {
			bits.Set(i)
		}
	}
}

func (m bytesInSetMatcher) matchBlockHashMapWithMask(b *block.Block, bits, mask *bitset.Bitset) {
	arr := b.Bytes()
	for i := range mask.Iterator() {
		if m.matchHashMap(arr.Get(i)) {
			bits.Set(i)
		}
	}
}

func (m bytesInSetMatcher) matchBlockSlice(b *block.Block, bits *bitset.Bitset) {
	for i, v := range b.Bytes().Iterator() {
		if m.slice.Contains(v) {
			bits.Set(i)
		}
	}
}

func (m bytesInSetMatcher) matchBlockSliceWithMask(b *block.Block, bits, mask *bitset.Bitset) {
	arr := b.Bytes()
	for i := range mask.Iterator() {
		if m.slice.Contains(arr.Get(i)) {
			bits.Set(i)
		}
	}
}

// NOT IN ---

type bytesNotInSetMatcher struct {
	bytesSetMatcher
}

func (m bytesNotInSetMatcher) MatchValue(v any) bool {
	return !m.slice.Contains(v.([]byte))
}

func (m bytesNotInSetMatcher) MatchRange(from, to any) bool {
	return !m.slice.ContainsRange(from.([]byte), to.([]byte))
}

func (m bytesNotInSetMatcher) MatchFilter(_ filter.Filter) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m bytesNotInSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if mask == nil {
		if m.hmap == nil {
			m.matchBlockSlice(b, bits)
		} else {
			m.matchBlockHashMap(b, bits)
		}
	} else {
		if m.hmap == nil {
			m.matchBlockSliceWithMask(b, bits, mask)
		} else {
			m.matchBlockHashMapWithMask(b, bits, mask)
		}
	}
}

func (m bytesNotInSetMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
}

func (m bytesNotInSetMatcher) matchBlockHashMap(b *block.Block, bits *bitset.Bitset) {
	for i, v := range b.Bytes().Iterator() {
		if !m.matchHashMap(v) {
			bits.Set(i)
		}
	}
}

func (m bytesNotInSetMatcher) matchBlockHashMapWithMask(b *block.Block, bits, mask *bitset.Bitset) {
	arr := b.Bytes()
	for i := range mask.Iterator() {
		if !m.matchHashMap(arr.Get(i)) {
			bits.Set(i)
		}
	}
}

func (m bytesNotInSetMatcher) matchBlockSlice(b *block.Block, bits *bitset.Bitset) {
	for i, v := range b.Bytes().Iterator() {
		if !m.slice.Contains(v) {
			bits.Set(i)
		}
	}
}

func (m bytesNotInSetMatcher) matchBlockSliceWithMask(b *block.Block, bits, mask *bitset.Bitset) {
	arr := b.Bytes()
	for i := range mask.Iterator() {
		if !m.slice.Contains(arr.Get(i)) {
			bits.Set(i)
		}
	}
}

// REGEXP ---

type bytesRegexpMatcher struct {
	noopMatcher
	re *regexp.Regexp
}

func (m *bytesRegexpMatcher) Value() any {
	if m.re == nil {
		return ""
	}
	return m.re.String()
}

func (m *bytesRegexpMatcher) Weight() int {
	return 100 // arbitrary cost
}

func (m *bytesRegexpMatcher) WithValue(v any) {
	var err error
	switch val := v.(type) {
	case []byte:
		m.re, err = regexp.Compile(string(val))
	case string:
		m.re, err = regexp.Compile(val)
	case *regexp.Regexp:
		m.re = val
	default:
		err = fmt.Errorf("unsupported regexp source type %T", v)
	}
	if err != nil {
		panic(err)
	}
}

func (m bytesRegexpMatcher) MatchValue(v any) bool {
	return m.re == nil || m.re.Match(v.([]byte))
}

func (m bytesRegexpMatcher) MatchRange(from, to any) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m bytesRegexpMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if m.re == nil {
		if mask != nil {
			bits.Copy(mask)
		}
		return
	}
	if mask != nil {
		arr := b.Bytes()
		for i := range mask.Iterator() {
			if m.re.Match(arr.Get(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range b.Bytes().Iterator() {
			if m.re.Match(v) {
				bits.Set(i)
			}
		}
	}
}

// TODO: prefix match might work
func (m bytesRegexpMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
}

func (m bytesRegexpMatcher) MatchFilter(_ filter.Filter) bool {
	// we don't know generally, so full scan is always required
	return true
}
