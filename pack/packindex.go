// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/vec"
)

type PackageHeader struct {
	Key          []byte                `json:"k"`
	NFields      int                   `json:"f"`
	NValues      int                   `json:"v"`
	PackSize     int                   `json:"s"`
	BlockHeaders block.BlockHeaderList `json:"b"`

	dirty bool
}

func (p *Package) Header() PackageHeader {
	h := PackageHeader{
		Key:          p.key,
		NFields:      p.nFields,
		NValues:      p.nValues,
		PackSize:     p.packedsize,
		BlockHeaders: make(block.BlockHeaderList, 0, p.nFields),
	}
	for _, v := range p.blocks {
		h.BlockHeaders = append(h.BlockHeaders, v.CloneHeader())
	}
	return h
}

func (h PackageHeader) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := h.Encode(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *PackageHeader) UnmarshalBinary(data []byte) error {
	return h.Decode(bytes.NewBuffer(data))
}

func (h PackageHeader) Encode(buf *bytes.Buffer) error {
	var b [8]byte
	i := binary.PutUvarint(b[:], uint64(len(h.Key)))
	_, _ = buf.Write(b[:i])
	_, _ = buf.Write(h.Key)
	bigEndian.PutUint32(b[0:], uint32(h.NFields))
	bigEndian.PutUint32(b[4:], uint32(h.NValues))
	buf.Write(b[:])
	bigEndian.PutUint32(b[0:], uint32(h.PackSize))
	buf.Write(b[:4])
	return h.BlockHeaders.Encode(buf)
}

func (h *PackageHeader) Decode(buf *bytes.Buffer) error {
	n, err := binary.ReadUvarint(buf)
	if err != nil {
		return fmt.Errorf("pack: reading pack header key: %v", err)
	}
	key := buf.Next(int(n))
	h.Key = make([]byte, len(key))
	copy(h.Key, key)
	b := buf.Next(8)
	h.NFields = int(bigEndian.Uint32(b[0:]))
	h.NValues = int(bigEndian.Uint32(b[4:]))
	b = buf.Next(4)
	h.PackSize = int(bigEndian.Uint32(b[0:]))
	return h.BlockHeaders.Decode(buf)
}

type PackageHeaderList []PackageHeader

func (l PackageHeaderList) Len() int           { return len(l) }
func (l PackageHeaderList) Less(i, j int) bool { return bytes.Compare(l[i].Key, l[j].Key) < 0 }
func (l PackageHeaderList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func (l *PackageHeaderList) Add(head PackageHeader) (PackageHeader, int, bool) {
	i := sort.Search(l.Len(), func(i int) bool {
		return bytes.Compare((*l)[i].Key, head.Key) >= 0
	})
	if i < len(*l) && bytes.Compare((*l)[i].Key, head.Key) == 0 {
		oldhead := (*l)[i]
		(*l)[i] = head
		return oldhead, i, false
	}
	*l = append(*l, PackageHeader{})
	copy((*l)[i+1:], (*l)[i:])
	(*l)[i] = head
	return PackageHeader{}, i, true
}

func (l *PackageHeaderList) Remove(head PackageHeader) (PackageHeader, int) {
	return l.RemoveKey(head.Key)
}

func (l *PackageHeaderList) RemoveKey(key []byte) (PackageHeader, int) {
	i := sort.Search(l.Len(), func(i int) bool {
		return bytes.Compare((*l)[i].Key, key) >= 0
	})
	if i < len(*l) && bytes.Compare((*l)[i].Key, key) == 0 {
		oldhead := (*l)[i]
		*l = append((*l)[:i], (*l)[i+1:]...)
		return oldhead, i
	}
	return PackageHeader{}, -1
}

func (h PackageHeaderList) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte(packageStorageFormatVersionV1)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(h)))
	buf.Write(b[:])
	for _, v := range h {
		if err := v.Encode(buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (h *PackageHeaderList) UnmarshalBinary(data []byte) error {
	if len(data) < 5 {
		return fmt.Errorf("pack: short package list header, length %d", len(data))
	}
	buf := bytes.NewBuffer(data)

	b, _ := buf.ReadByte()
	if b != packageStorageFormatVersionV1 {
		return fmt.Errorf("pack: invalid package list header version %d", b)
	}

	l := int(binary.BigEndian.Uint32(buf.Next(4)))

	*h = make(PackageHeaderList, l)

	for i := range *h {
		if err := (*h)[i].Decode(buf); err != nil {
			return err
		}
	}
	return nil
}

// PackIndex implements efficient and scalable pack header management as
// well as primary key placement (i.e. best pack selection).
//
// Packs are stored and referenced in key order ([4]byte = big endian uint32),
// by tables and indexes, but primary keys do not necessarily have a global order
// across packs. This means pk min-max ranges are unordered and we have to map
// from pk to packs in queries and flush operations.
//
// PackIndex takes care of searching a pack that contains a given pk and selects
// the best pack to store a new pk into. New or updated packs (i.e. from calls to split
// or storePack) are registered using their headers. When empty packs are deleted
// (Table only) they must be deregistered.
//
// PackIndex keeps a list of all current pack headers (heads) and a list of all
// removed header keys in memory. storePackHeader calls use these lists to update
// the stored representation.
//
type PackIndex struct {
	heads  PackageHeaderList
	minpks []uint64
	maxpks []uint64
	deads  vec.ByteSlice
	pairs  []pair
	pkidx  int
}

type pair struct {
	min uint64 // minimum value used as sort criteria
	pos int    // position in heads list
}

// may be used in {Index|Table}.loadPackHeaders
func NewPackIndex(heads PackageHeaderList, pkidx int) *PackIndex {
	if heads == nil {
		heads = make(PackageHeaderList, 0)
	}
	l := &PackIndex{
		heads:  heads,
		minpks: make([]uint64, len(heads), cap(heads)),
		maxpks: make([]uint64, len(heads), cap(heads)),
		deads:  make(vec.ByteSlice, 0),
		pkidx:  pkidx,
		pairs:  make([]pair, len(heads), cap(heads)),
	}
	sort.Sort(l.heads)
	for i := range l.heads {
		l.minpks[i] = l.heads[i].BlockHeaders[l.pkidx].MinValue.(uint64)
		l.maxpks[i] = l.heads[i].BlockHeaders[l.pkidx].MaxValue.(uint64)
		l.pairs[i].min = l.minpks[i]
		l.pairs[i].pos = i
	}
	l.Sort()
	return l
}

func (l *PackIndex) Len() int {
	return len(l.heads)
}

func (l *PackIndex) Sort() {
	sort.Slice(l.pairs, func(i, j int) bool { return l.pairs[i].min < l.pairs[j].min })
}

func (l *PackIndex) MinMax(n int) (uint64, uint64) {
	if n >= l.Len() {
		return 0, 0
	}
	return l.minpks[n], l.maxpks[n]
}

func (l *PackIndex) GlobalMinMax() (uint64, uint64) {
	if l.Len() == 0 {
		return 0, 0
	}
	pos := l.pairs[len(l.pairs)-1].pos
	return l.minpks[pos], l.maxpks[pos]
}

func (l *PackIndex) MinMaxSlices() ([]uint64, []uint64) {
	return l.minpks, l.maxpks
}

func (l *PackIndex) Get(i int) PackageHeader {
	if i < 0 || i >= l.Len() {
		return PackageHeader{}
	}
	return l.heads[i]
}

// called by storePack
func (l *PackIndex) AddOrUpdate(head PackageHeader) {
	head.dirty = true
	l.deads.Remove(head.Key)
	old, pos, isAdd := l.heads.Add(head)

	// keep positions of heads in l.heads in sync with positions stored in l.pairs
	// keep min values in l.pairs in sync with primary key min values in heads
	if isAdd {
		var needsort bool

		// appends of new packs can use a more efficient implementation
		if pos > 0 && pos == l.Len()-1 {
			// get the added heads min and the highest max of all managed heads
			newmin := l.heads[pos].BlockHeaders[l.pkidx].MinValue.(uint64)
			newmax := l.heads[pos].BlockHeaders[l.pkidx].MaxValue.(uint64)
			lastmax := l.heads[l.pairs[len(l.pairs)-1].pos].BlockHeaders[l.pkidx].MaxValue.(uint64)

			// simple appends of packs with higher min than any existing max does
			// not require re-sorting the pair slice
			needsort = newmin < lastmax

			// append new pair
			l.pairs = append(l.pairs, pair{
				min: newmin,
				pos: pos,
			})
			l.minpks = append(l.minpks, newmin)
			l.maxpks = append(l.maxpks, newmax)

		} else {
			// all header positions after `pos` have shifted right, so we must update
			// all corresponding pairs as well; for simplicity we rebuild the entire
			// pairs slice from scratch

			// grow pairs if necessary
			if cap(l.pairs) < len(l.heads) {
				l.pairs = make([]pair, len(l.heads))
				l.minpks = make([]uint64, len(l.heads))
				l.maxpks = make([]uint64, len(l.heads))
			}
			l.pairs = l.pairs[:len(l.heads)]
			l.minpks = l.minpks[:len(l.heads)]
			l.maxpks = l.maxpks[:len(l.heads)]
			for i := range l.heads {
				l.minpks[i] = l.heads[i].BlockHeaders[l.pkidx].MinValue.(uint64)
				l.maxpks[i] = l.heads[i].BlockHeaders[l.pkidx].MaxValue.(uint64)
				l.pairs[i].min = l.minpks[i]
				l.pairs[i].pos = i
			}
			needsort = true
		}
		// resort the pairs slice by min value
		if needsort {
			sort.Slice(l.pairs, func(i, j int) bool { return l.pairs[i].min < l.pairs[j].min })
		}
	} else {
		// update min/max pk slices to stay in sync with pack headers
		newmin := l.heads[pos].BlockHeaders[l.pkidx].MinValue.(uint64)
		newmax := l.heads[pos].BlockHeaders[l.pkidx].MaxValue.(uint64)
		l.minpks[pos] = newmin
		l.maxpks[pos] = newmax

		// skip pair update if min value hasn't changed
		oldmin := old.BlockHeaders[l.pkidx].MinValue.(uint64)
		if newmin == oldmin {
			return
		}

		// find and update the pair at position pos; since we know it's old min
		// value we can use binary search; also we know the value exists, so we
		// can skip checking i
		i := sort.Search(l.Len(), func(i int) bool { return l.pairs[i].min >= oldmin })
		if i < l.Len() && l.pairs[i].min == oldmin {
			l.pairs[i].min = newmin
		} else {
			log.Warnf("pack: pack header index update mismatch: old-pack=%x new-pack=%x old-min=%d new-min=%d index=%d/%d",
				old.Key, head.Key, old.BlockHeaders[l.pkidx].MinValue.(uint64), newmin, i, l.Len())
		}

		// it's guaranteed that a change in min value cannot make the list unsorted
		// so we can safely skip sorting here
	}
}

// called by storePack when packs are empty (Table only)
func (l *PackIndex) Remove(head PackageHeader) {
	oldhead, pos := l.heads.RemoveKey(head.Key)
	if pos < 0 {
		return
	}
	// store as dead key
	l.deads.AddUnique(head.Key)

	// remove pos from min/max slices
	l.minpks = append(l.minpks[:pos], l.minpks[pos+1:]...)
	l.maxpks = append(l.maxpks[:pos], l.maxpks[pos+1:]...)

	// when just the trailing head has been removed we can use a more efficient
	// algorithm because head positions haven't changed
	if pos > 0 && pos == l.Len() {
		// find the pair to remove based on its min value using binary search
		// we know the pair must exist
		min := oldhead.BlockHeaders[l.pkidx].MinValue.(uint64)
		i := sort.Search(l.Len(), func(i int) bool { return l.pairs[i].min >= min })
		l.pairs = append(l.pairs[:i], l.pairs[i+1:]...)

	} else {
		// all header positions after `pos` have shifted left, so we must update
		// all corresponding pairs as well; for simplicity we rebuild the entire
		// pairs slice from scratch
		l.pairs = l.pairs[:len(l.heads)]
		for i := range l.heads {
			l.pairs[i].min = l.heads[i].BlockHeaders[l.pkidx].MinValue.(uint64)
			l.pairs[i].pos = i
		}

		// resort the pairs slice by min value
		sort.Slice(l.pairs, func(i, j int) bool { return l.pairs[i].min < l.pairs[j].min })
	}
}

// assumes pair list is sorted by min value and pack min/max ranges don't overlap
// this is the case for all index and table packs because the placement algorithm
// and the splitting algorithm make sure overlaps never exist
func (l *PackIndex) Best(val uint64) (int, uint64, uint64) {
	numpacks := l.Len()

	// initially we stick to the first pack until split
	if numpacks == 0 {
		return 0, 0, 0
	}

	// find first pack with min larger than value
	i := sort.Search(numpacks, func(i int) bool { return l.pairs[i].min > val })

	// assign value to the previous pack or the very first pack
	// note that when value is larger than any pack's min value
	// it is assign to last pack
	if i > 0 {
		i--
	}

	// return the pack's list position and the corresponding min/max header values
	pos := l.pairs[i].pos
	return pos, l.minpks[pos], l.maxpks[pos]
}
