// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc, alex@blockwatch.cc

// Positional SMA implementation inspired by
//
// Harald Lang, Tobias Mühlbauer, Florian Funke, Peter A. Boncz
// Data Blocks: Hybrid OLTP and OLAP on Compressed Storage using
// both Vectorization and Compilation
// http://dx.doi.org/10.1145/2882903.2882925

package stats

import (
	"errors"
	"math/bits"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/constraints"
)

var ErrUnsupportedType = errors.New("range index: unsupported data type")

type RangeIndex struct {
	buf   []byte
	lower []uint32
	upper []uint32
}

func BuildRangeIndex(b *block.Block, minVal, maxVal any) (*RangeIndex, error) {
	switch b.Type() {
	case types.BlockInt64:
		return buildRangeIndex(b.Int64().Slice(), minVal.(int64), maxVal.(int64)), nil
	case types.BlockInt32:
		return buildRangeIndex(b.Int32().Slice(), minVal.(int32), maxVal.(int32)), nil
	case types.BlockInt16:
		return buildRangeIndex(b.Int16().Slice(), minVal.(int16), maxVal.(int16)), nil
	case types.BlockInt8:
		return buildRangeIndex(b.Int8().Slice(), minVal.(int8), maxVal.(int8)), nil
	case types.BlockUint64:
		return buildRangeIndex(b.Uint64().Slice(), minVal.(uint64), maxVal.(uint64)), nil
	case types.BlockUint32:
		return buildRangeIndex(b.Uint32().Slice(), minVal.(uint32), maxVal.(uint32)), nil
	case types.BlockUint16:
		return buildRangeIndex(b.Uint16().Slice(), minVal.(uint16), maxVal.(uint16)), nil
	case types.BlockUint8:
		return buildRangeIndex(b.Uint8().Slice(), minVal.(uint8), maxVal.(uint8)), nil
	default:
		return nil, ErrUnsupportedType
	}
}

func RangeIndexFromBytes(buf []byte) *RangeIndex {
	return &RangeIndex{
		buf:   buf,
		lower: util.FromByteSlice[uint32](buf[:len(buf)/2]),
		upper: util.FromByteSlice[uint32](buf[len(buf)/2:]),
	}
}

func (idx *RangeIndex) Close() {
	idx.buf = nil
	idx.lower = nil
	idx.upper = nil
}

func (idx RangeIndex) IsValid() bool {
	return len(idx.buf) > 0
}

func (idx RangeIndex) NumSlots() int {
	return len(idx.lower)
}

func (idx RangeIndex) Data() ([]uint32, []uint32) {
	return idx.lower, idx.upper
}

func (idx RangeIndex) NumUsedSlots() int {
	var n int
	for _, v := range idx.upper {
		n += util.Bool2int(v > 0)
	}
	return n
}

func (idx RangeIndex) NumGroups() int {
	return (len(idx.lower) + 255) / 256
}

func (idx RangeIndex) Size() int {
	return len(idx.buf)
}

func (idx RangeIndex) Bytes() []byte {
	return idx.buf
}

func (idx RangeIndex) Range(val, minVal int) types.Range {
	slot, ok := getSlot(val, minVal)
	if !ok || slot >= len(idx.lower) {
		return types.InvalidRange
	}
	return types.Range{idx.lower[slot], idx.upper[slot] - 1}
}

// Query returns a vector range according to filter mode and value. Note that
// value can be outside of the learned filter range, e.g. value can be smaller
// than vector min or larger than vector max. Since the index does not store
// vector max and we only have fuzzy ranges for the top-most slot a slightly
// out of range upper bound on range/in/greater queries would not perfectly
// be detected as out of bounds. Given the imprecise nature for large values
// in general this is still acceptable.
func (idx RangeIndex) Query(flt *filter.Filter, minVal any, nRows int) types.Range {
	switch flt.Mode {
	case types.FilterModeEqual:
		slot, ok := getSlotTyped(flt.Type, flt.Value, minVal)
		if !ok || slot >= len(idx.lower) {
			return types.InvalidRange
		}
		return types.Range{idx.lower[slot], idx.upper[slot] - 1}

	case types.FilterModeLt:
		endSlot, ok := getSlotTyped(flt.Type, flt.Type.Dec(flt.Value), minVal)
		if !ok {
			return types.InvalidRange
		}
		return idx.mergeRange(0, endSlot, uint32(nRows))

	case types.FilterModeLe:
		endSlot, ok := getSlotTyped(flt.Type, flt.Value, minVal)
		if !ok {
			return types.InvalidRange
		}
		return idx.mergeRange(0, endSlot, uint32(nRows))

	case types.FilterModeGt:
		startSlot, ok := getSlotTyped(flt.Type, flt.Type.Inc(flt.Value), minVal)
		if ok && startSlot >= len(idx.lower) {
			return types.InvalidRange
		} else if !ok {
			startSlot = 0
		}
		return idx.mergeRange(startSlot, idx.NumSlots()-1, uint32(nRows))

	case types.FilterModeGe:
		startSlot, ok := getSlotTyped(flt.Type, flt.Value, minVal)
		if ok && startSlot >= len(idx.lower) {
			return types.InvalidRange
		} else if !ok {
			startSlot = 0
		}
		return idx.mergeRange(startSlot, idx.NumSlots()-1, uint32(nRows))

	case types.FilterModeRange:
		rv := flt.Value.(filter.RangeValue)
		startSlot, ok := getSlotTyped(flt.Type, rv[0], minVal)
		if ok && startSlot >= len(idx.lower) {
			return types.InvalidRange
		} else if !ok {
			startSlot = 0
		}
		endSlot, ok := getSlotTyped(flt.Type, rv[1], minVal)
		if !ok {
			return types.InvalidRange
		}
		return idx.mergeRange(startSlot, endSlot, uint32(nRows))

	case types.FilterModeIn:
		// filter set -> min/max -> range
		// note filters store sets as sorted slices, matchers as xroar bitmap
		minFlt, maxFlt, _ := flt.Type.Range(flt.Value)
		startSlot, ok := getSlotTyped(flt.Type, minFlt, minVal)
		if ok && startSlot >= len(idx.lower) {
			return types.InvalidRange
		} else if !ok {
			startSlot = 0
		}
		endSlot, ok := getSlotTyped(flt.Type, maxFlt, minVal)
		if !ok {
			return types.InvalidRange
		}
		return idx.mergeRange(startSlot, endSlot, uint32(nRows))

	default:
		// other filter modes are unsupported (NE, NIN)
		return types.Range{0, uint32(nRows)}
	}
}

func (idx RangeIndex) mergeRange(start, end int, maxRange uint32) types.Range {
	// bounds check
	l := idx.NumSlots()
	if start >= l {
		return types.InvalidRange
	}
	end = min(end, l-1)

	var lower uint32 = 1<<32 - 1
	for i, v := range idx.lower[start : end+1] {
		// skip unused slots
		if idx.upper[start+i] == 0 {
			continue
		}
		lower = min(lower, v)

		// stop at lower bound
		if lower == 0 {
			break
		}
	}

	// reset when no match was found
	if lower == 1<<32-1 {
		lower = 0
	}

	var upper uint32
	for _, v := range idx.upper[start : end+1] {
		upper = max(upper, v)

		// stop at upper bound
		if upper == maxRange {
			break
		}
	}

	// when 0,0 returns equivalent to InvalidRange(0, 1<<32-1)
	return types.Range{lower, upper - 1}
}

func buildRangeIndex[T constraints.Integer](src []T, minVal, maxVal T) *RangeIndex {
	nSlots, _ := getSlot(maxVal, minVal) // highest used slot (zero based array index)
	nSlots++                             // add one for correct array size
	buf := make([]byte, nSlots*8)        // 2x uint32
	idx := &RangeIndex{
		buf:   buf,
		lower: util.FromByteSlice[uint32](buf[:nSlots*4]),
		upper: util.FromByteSlice[uint32](buf[nSlots*4:]),
	}
	for i, v := range src {
		slot, _ := getSlot(v, minVal)
		if idx.upper[slot] == 0 {
			idx.lower[slot] = uint32(i)
		}
		idx.upper[slot] = uint32(i + 1)
	}
	return idx
}

// Returns calculated slot and whether val underflows (val<min).
func getSlotTyped(typ types.BlockType, val, minVal any) (int, bool) {
	switch typ {
	case types.BlockInt64:
		return getSlot(val.(int64), minVal.(int64))
	case types.BlockInt32:
		return getSlot(val.(int32), minVal.(int32))
	case types.BlockInt16:
		return getSlot(val.(int16), minVal.(int16))
	case types.BlockInt8:
		return getSlot(val.(int8), minVal.(int8))
	case types.BlockUint64:
		return getSlot(val.(uint64), minVal.(uint64))
	case types.BlockUint32:
		return getSlot(val.(uint32), minVal.(uint32))
	case types.BlockUint16:
		return getSlot(val.(uint16), minVal.(uint16))
	case types.BlockUint8:
		return getSlot(val.(uint8), minVal.(uint8))
	default:
		return -1, false
	}
}

// Returns calculated slot and whether val underflows (val<min).
func getSlot[T constraints.Integer](val, minVal T) (int, bool) {
	delta := uint64(val - minVal)
	var r int
	if delta != 0 {
		zero := bits.LeadingZeros64(delta)
		r = 7 - zero>>3
	}
	return int(delta>>(r<<3)) + (r << 8), val >= minVal
}
