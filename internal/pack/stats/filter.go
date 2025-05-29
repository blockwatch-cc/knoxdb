// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"slices"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/filter/fuse"
	"blockwatch.cc/knoxdb/internal/filter/llb"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

func filterKey(pkey uint32, fx uint16) []byte {
	var b [num.MaxVarintLen32 + num.MaxVarintLen16]byte
	return num.AppendUvarint(
		num.AppendUvarint(b[:0], uint64(pkey)),
		uint64(fx),
	)
}

func (idx Index) buildFilters(pkg *pack.Package, node *SNode) error {
	// access statistics
	n, ok := node.FindKey(pkg.Key())
	if !ok {
		return nil
	}

	// build filters in mem and save later
	blooms := make(map[uint16][]byte)
	bits := make(map[uint16][]byte)
	fuses := make(map[uint16][]byte)
	ranges := make(map[uint16][]byte)
	for i, f := range pkg.Schema().Exported() {
		b := pkg.Block(i)
		if b == nil || !b.IsDirty() {
			continue
		}

		switch f.Index {
		case types.IndexTypeBloom:
			if idx.use.Is(FeatBloomFilter) {
				// TODO: use cardinality and unique values from pack analyis step
				card := EstimateCardinality(b, 8)
				if flt := BuildBloomFilter(b, card, int(f.Scale)); flt != nil {
					blooms[uint16(i)] = flt.Bytes()
				}
			}
		case types.IndexTypeBfuse:
			if idx.use.Is(FeatFuseFilter) {
				// TODO: use unique values from pack analyis step
				flt, err := BuildFuseFilter(b)
				if err != nil {
					return err
				}
				fuses[uint16(i)], _ = flt.MarshalBinary()
			}
		case types.IndexTypeBits:
			if idx.use.Is(FeatBitsFilter) {
				// TODO: use cardinality and unique values from pack analyis step
				card := EstimateCardinality(b, 8)
				if flt := BuildBitsFilter(b, card); flt != nil {
					bits[uint16(i)] = flt.ToBuffer()
				}
			}
		}

		// build range filters for int columns
		if b.Type().IsInt() && idx.use.Is(FeatRangeFilter) {
			minv := node.spack.Block(minColIndex(i)).Get(n)
			maxv := node.spack.Block(maxColIndex(i)).Get(n)
			rg, err := BuildRangeIndex(b, minv, maxv)
			if err != nil {
				return err
			}
			ranges[uint16(i)] = rg.Bytes()
		}
	}

	// early exit
	if len(blooms)+len(ranges)+len(bits)+len(fuses) == 0 {
		return nil
	}

	// store filters
	return idx.db.Update(func(tx store.Tx) error {
		// create stats buckets if not exist
		for k := 0; k < STATS_BUCKETS; k++ {
			_, err := tx.Root().CreateBucketIfNotExists(idx.keys[k])
			if err != nil {
				return err
			}
		}

		if len(blooms) > 0 {
			b := idx.bloomBucket(tx)
			if b == nil {
				return store.ErrNoBucket
			}
			for k, buf := range blooms {
				key := filterKey(pkg.Key(), k)
				err := b.Put(key, buf)
				if err != nil {
					return err
				}
				idx.bytesWritten += int64(len(buf))
			}
		}
		clear(blooms)

		if len(ranges) > 0 {
			b := idx.rangeBucket(tx)
			if b == nil {
				return store.ErrNoBucket
			}
			for k, buf := range ranges {
				key := filterKey(pkg.Key(), k)
				err := b.Put(key, buf)
				if err != nil {
					return err
				}
				idx.bytesWritten += int64(len(buf))
			}
		}
		clear(ranges)

		if len(bits) > 0 {
			b := idx.bitsBucket(tx)
			if b == nil {
				return store.ErrNoBucket
			}
			for k, buf := range bits {
				key := filterKey(pkg.Key(), k)
				err := b.Put(key, buf)
				if err != nil {
					return err
				}
				idx.bytesWritten += int64(len(buf))
			}
		}
		clear(bits)

		if len(fuses) > 0 {
			b := idx.fuseBucket(tx)
			if b == nil {
				return store.ErrNoBucket
			}
			for k, buf := range fuses {
				key := filterKey(pkg.Key(), k)
				err := b.Put(key, buf)
				if err != nil {
					return err
				}
				idx.bytesWritten += int64(len(buf))
			}
		}
		clear(fuses)

		return nil
	})

}

func (idx Index) dropFilters(pkg *pack.Package) error {
	// delete bloom and range filters using pkg key as prefix
	return idx.db.Update(func(tx store.Tx) error {
		prefix := num.EncodeUvarint(uint64(pkg.Key()))
		for _, k := range []int{
			STATS_BLOOM_KEY,
			STATS_RANGE_KEY,
			STATS_BITS_KEY,
			STATS_FUSE_KEY,
		} {
			b := idx.bucket(tx, k)
			if b == nil {
				return store.ErrNoBucket
			}
			c := b.Range(prefix)
			for ok := c.First(); ok; ok = c.Next() {
				_ = b.Delete(c.Key())
			}
			c.Close()
		}
		return nil
	})
}

func EstimateCardinality(b *block.Block, precision int) int {
	// shortcut for empty and very small blocks
	l := b.Len()
	switch l {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		minVal, maxVal := b.MinMax()
		if b.Type().EQ(minVal, maxVal) {
			return 1
		}
		return 2
	}

	// type-based estimation
	// - use llb for 256/128/64/32 bit numbers and bytes/strings
	// - use xroar bitmaps for 16/8 bit
	switch b.Type() {
	case block.BlockInt64, block.BlockUint64, block.BlockFloat64:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		flt.AddMultiUint64(b.Uint64().Slice())
		return min(l, int(flt.Cardinality()))

	case block.BlockInt32, block.BlockUint32, block.BlockFloat32:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		flt.AddMultiUint32(b.Uint32().Slice())
		return min(l, int(flt.Cardinality()))

	case block.BlockInt16, block.BlockUint16:
		bits := xroar.NewWithSize(l)
		for _, v := range b.Uint16().Slice() {
			bits.Set(uint64(v))
		}
		return bits.Count()

	case block.BlockInt8, block.BlockUint8:
		bits := xroar.NewWithSize(l)
		for _, v := range b.Uint8().Slice() {
			bits.Set(uint64(v))
		}
		return bits.Count()

	case block.BlockInt256:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		for _, v := range b.Int256().Iterator() {
			flt.Add(v.Bytes())
		}
		return min(l, int(flt.Cardinality()))

	case block.BlockInt128:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		for _, v := range b.Int128().Iterator() {
			flt.Add(v.Bytes())
		}
		return min(l, int(flt.Cardinality()))

	case block.BlockBytes:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		for _, v := range b.Bytes().Iterator() {
			flt.Add(v)
		}
		return min(l, int(flt.Cardinality()))

	case block.BlockBool:
		min, max := b.MinMax()
		if min == max {
			return 1
		}
		return 2

	default:
		return 0
	}
}

func BuildBloomFilter(b *block.Block, cardinality, factor int) *bloom.Filter {
	if cardinality <= 0 || factor <= 0 {
		return nil
	}

	// dimension filter for cardinality and factor to control its false positive rate
	// (bloom expects size in bits)
	//
	// - 2% for m = set cardinality * 2
	// - 0.2% for m = set cardinality * 3
	// - 0.02% for m = set cardinality * 4
	flt := bloom.NewFilter(cardinality * factor * 8)

	switch b.Type() {
	case block.BlockInt64, block.BlockUint64:
		// we write uint64 data in little endian order into the filter,
		// so all 8 byte numeric types look the same (float64 uses FloatBits == uint64)
		flt.AddManyUint64(b.Uint64().Slice())

	case block.BlockInt32, block.BlockUint32:
		// we write uint32 data in little endian order into the filter,
		// so all 4 byte numeric types look the same (float32 uses FloatBits == uint32)
		flt.AddManyUint32(b.Uint32().Slice())

	case block.BlockInt16, block.BlockUint16:
		// we write uint16 data in little endian order into the filter,
		// so all 2 byte numeric types look the
		flt.AddManyUint16(b.Uint16().Slice())

	case block.BlockInt8, block.BlockUint8:
		flt.AddManyUint8(b.Uint8().Slice())

	case block.BlockInt256:
		// write individual elements (no optimization exists)
		for _, v := range b.Int256().Iterator() {
			flt.Add(v.Bytes())
		}

	case block.BlockInt128:
		// write individual elements (no optimization exists)
		for _, v := range b.Int128().Iterator() {
			flt.Add(v.Bytes())
		}

	case block.BlockBytes:
		// write only unique elements (post-dedup optimization this avoids
		// calculating hashes for duplicates)
		for _, v := range b.Bytes().Iterator() {
			flt.Add(v)
		}

	default:
		// BlockFloat32/64, BlockBool and unknown/future types have no filter
		return nil
	}
	return flt
}

func BuildBitsFilter(b *block.Block, cardinality int) *xroar.Bitmap {
	if cardinality <= 1 {
		return nil
	}

	flt := xroar.NewWithSize(cardinality)

	switch b.Type() {
	case block.BlockInt64, block.BlockUint64:
		for _, v := range b.Uint64().Slice() {
			flt.Set(v)
		}

	case block.BlockInt32, block.BlockUint32:
		for _, v := range b.Uint32().Slice() {
			flt.Set(uint64(v))
		}

	case block.BlockInt16, block.BlockUint16:
		for _, v := range b.Uint16().Slice() {
			flt.Set(uint64(v))
		}

	case block.BlockInt8, block.BlockUint8:
		for _, v := range b.Uint8().Slice() {
			flt.Set(uint64(v))
		}

	default:
		// unsupported
		// BlockInt256, BlockInt128, BlockBytes, BlockBool, BlockFloat32/64
		// unknown/future types have no filter
		return nil
	}
	return flt
}

func BuildFuseFilter(b *block.Block) (*fuse.BinaryFuse[uint8], error) {
	if !b.IsMaterialized() {
		return nil, block.ErrBlockNotMaterialized
	}
	// create a private data copy (for unique algos)
	var u64 []uint64
	switch b.Type() {
	case block.BlockInt64, block.BlockUint64:
		u64 = slices.Clone(b.Uint64().Slice())

	case block.BlockInt32, block.BlockUint32:
		u64 = util.ConvertSlice[uint32, uint64](b.Uint32().Slice())

	case block.BlockInt16, block.BlockUint16:
		u64 = util.ConvertSlice[uint16, uint64](b.Uint16().Slice())

	case block.BlockInt8, block.BlockUint8:
		u64 = util.ConvertSlice[uint8, uint64](b.Uint8().Slice())

	case block.BlockInt256:
		// write individual elements (no optimization exists)
		u64 = make([]uint64, b.Len())
		for i, v := range b.Int256().Iterator() {
			u64[i] = filter.Hash(v.Bytes()).Uint64()
		}

	case block.BlockInt128:
		// write individual elements (no optimization exists)
		u64 = make([]uint64, b.Len())
		for i, v := range b.Int128().Iterator() {
			u64[i] = filter.Hash(v.Bytes()).Uint64()
		}

	case block.BlockBytes:
		// write all strings
		u64 = make([]uint64, b.Len())
		for i, v := range b.Bytes().Iterator() {
			u64[i] = filter.Hash(v).Uint64()
		}

	default:
		// BlockFloat32/64, BlockBool and unknown/future types have no filter
		return nil, schema.ErrInvalidValueType
	}

	// need unique values for filter construction
	u64 = slicex.Unique(u64)
	return fuse.NewBinaryFuse[uint8](u64)
}
