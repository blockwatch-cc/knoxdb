// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"slices"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/filter/fuse"
	"blockwatch.cc/knoxdb/internal/filter/llb"
	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
)

// key = [field_id:pack_id:version], cluster filter types in storage pages
func encodeFilterKey(pkey, ver uint32, fx uint16) []byte {
	var b [num.MaxVarintLen32 + 2*num.MaxVarintLen16]byte
	buf := num.AppendUvarint(b[:0], uint64(fx))
	buf = num.AppendUvarint(buf, uint64(pkey))
	buf = num.AppendUvarint(buf, uint64(ver&0xFFFF))
	return buf
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
	pstats := pkg.Stats()
	for i, f := range pkg.Schema().Fields {
		b := pkg.Block(i)
		if b == nil || !pstats.WasDirty[i] {
			continue
		}

		switch f.Filter {
		case types.FilterTypeBloom2b, types.FilterTypeBloom3b,
			types.FilterTypeBloom4b, types.FilterTypeBloom5b:
			if idx.use.Is(FeatBloomFilter) {
				// use cardinality from pack analyze step if exists, otherwise
				// fall back to cardinality estimation and reuse hashes
				card := pstats.Unique[i]
				var hashes []uint64
				if card <= 0 {
					card, hashes = EstimateCardinality(b, 8)
				}
				if flt := BuildBloomFilter(b, card, f.Filter.Factor(), hashes); flt != nil {
					blooms[f.Id] = flt.Bytes()
				}
			}
		case types.FilterTypeBfuse8:
			if idx.use.Is(FeatFuseFilter) {
				// TODO: use unique values from pack analyis step
				flt, err := BuildFuseFilter[uint8](b)
				if err != nil {
					return err
				}
				fuses[f.Id], _ = flt.MarshalBinary()
			}
		case types.FilterTypeBfuse16:
			if idx.use.Is(FeatFuseFilter) {
				// TODO: use unique values from pack analyis step
				flt, err := BuildFuseFilter[uint16](b)
				if err != nil {
					return err
				}
				fuses[f.Id], _ = flt.MarshalBinary()
			}
		case types.FilterTypeBits:
			if idx.use.Is(FeatBitsFilter) {
				// use cardinality from pack analyze step if exists, otherwise
				// fall back to cardinality estimation
				card := pstats.Unique[i]
				if card <= 0 {
					card, _ = EstimateCardinality(b, 8)
				}
				if flt := BuildBitsFilter(b, card); flt != nil {
					bits[f.Id] = flt.Bytes()
				}
			}
		}

		// build range filters for int columns
		if b.Type().IsInt() && idx.use.Is(FeatRangeFilter) {
			pkg := node.spack.Load()
			minv := pkg.Block(minColIndex(i)).Get(n)
			maxv := pkg.Block(maxColIndex(i)).Get(n)
			rg, err := BuildRangeIndex(b, minv, maxv)
			if err != nil {
				return err
			}
			ranges[f.Id] = rg.Bytes()
		}
	}

	// early exit
	if len(blooms)+len(ranges)+len(bits)+len(fuses) == 0 {
		return nil
	}

	// store filters
	return idx.db.Update(func(tx store.Tx) error {
		if len(blooms) > 0 {
			b := idx.filterBucket(tx)
			if b == nil {
				return store.ErrBucketNotFound
			}
			for k, buf := range blooms {
				key := encodeFilterKey(pkg.Key(), pkg.Version(), k)
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
				return store.ErrBucketNotFound
			}
			for k, buf := range ranges {
				key := encodeFilterKey(pkg.Key(), pkg.Version(), k)
				err := b.Put(key, buf)
				if err != nil {
					return err
				}
				idx.bytesWritten += int64(len(buf))
			}
		}
		clear(ranges)

		if len(bits) > 0 {
			b := idx.filterBucket(tx)
			if b == nil {
				return store.ErrBucketNotFound
			}
			for k, buf := range bits {
				key := encodeFilterKey(pkg.Key(), pkg.Version(), k)
				err := b.Put(key, buf)
				if err != nil {
					return err
				}
				idx.bytesWritten += int64(len(buf))
			}
		}
		clear(bits)

		if len(fuses) > 0 {
			b := idx.filterBucket(tx)
			if b == nil {
				return store.ErrBucketNotFound
			}
			for k, buf := range fuses {
				key := encodeFilterKey(pkg.Key(), pkg.Version(), k)
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
		for _, k := range []int{
			STATS_FILTER_KEY,
			STATS_RANGE_KEY,
		} {
			b := idx.bucket(tx, k)
			if b == nil {
				return store.ErrBucketNotFound
			}
			for _, f := range pkg.Schema().Fields {
				if f.Filter == 0 {
					continue
				}
				_ = b.Delete(encodeFilterKey(pkg.Key(), pkg.Version(), f.Id))
			}
		}
		return nil
	})
}

func EstimateCardinality(b *block.Block, precision int) (int, []uint64) {
	// shortcut for empty and very small blocks
	l := b.Len()
	switch l {
	case 0:
		return 0, nil
	case 1:
		return 1, nil
	case 2:
		minVal, maxVal := b.MinMax()
		if b.Type().EQ(minVal, maxVal) {
			return 1, nil
		}
		return 2, nil
	}

	// type-based estimation
	// - use llb for 256/128/64/32 bit numbers and bytes/strings
	// - use xroar bitmaps for 16/8 bit
	switch b.Type() {
	case block.BlockInt64, block.BlockUint64, block.BlockFloat64:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		hashes := hash.Vec64(b.Uint64().Slice(), arena.AllocUint64(l))
		flt.Add(hashes...)
		return min(l, int(flt.Cardinality())), hashes

	case block.BlockInt32, block.BlockUint32, block.BlockFloat32:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		hashes := hash.Vec32(b.Uint32().Slice(), arena.AllocUint64(l))
		flt.Add(hashes...)
		return min(l, int(flt.Cardinality())), hashes

	case block.BlockInt16, block.BlockUint16:
		bits := xroar.NewWithSize(l)
		for _, v := range b.Uint16().Slice() {
			bits.Set(uint64(v))
		}
		return bits.Count(), nil

	case block.BlockInt8, block.BlockUint8:
		bits := xroar.NewWithSize(l)
		for _, v := range b.Uint8().Slice() {
			bits.Set(uint64(v))
		}
		return bits.Count(), nil

	case block.BlockInt256:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		hashes := arena.AllocUint64(l)[:l]
		for i, v := range b.Int256().Iterator() {
			hashes[i] = hash.Hash(v.Bytes())
		}
		flt.Add(hashes...)
		return min(l, int(flt.Cardinality())), hashes

	case block.BlockInt128:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		hashes := arena.AllocUint64(l)[:l]
		for i, v := range b.Int128().Iterator() {
			hashes[i] = hash.Hash(v.Bytes())
		}
		flt.Add(hashes...)
		return min(l, int(flt.Cardinality())), hashes

	case block.BlockBytes:
		flt := llb.NewFilterWithPrecision(uint32(precision))
		hashes := arena.AllocUint64(l)[:l]
		for i, v := range b.Bytes().Iterator() {
			hashes[i] = hash.Hash(v)
		}
		flt.Add(hashes...)
		return min(l, int(flt.Cardinality())), hashes

	case block.BlockBool:
		min, max := b.MinMax()
		if min == max {
			return 1, nil
		}
		return 2, nil

	default:
		return 0, nil
	}
}

func BuildBloomFilter(b *block.Block, cardinality int, factor int, hashes []uint64) *bloom.Filter {
	if cardinality <= 0 || factor <= 0 {
		return nil
	}

	// dimension filter for cardinality and factor to control its
	// false positive rate (bloom.NewFilter expects size in bits)
	// factor directly controls filter size in bytes per value
	//
	// factor   p          p(%)      false positive rate
	// -------------------------------------------------
	// 1        0.023968   2.4%      1 in 42
	// 2        0.002394   0.2%      1 in 418
	// 3        0.000555   0.05%     1 in 1,800
	// 4        0.000190   0.02%     1 in 5,246
	// 5        0.000082   0.008%    1 in 12,194
	flt := bloom.NewFilter(cardinality * factor * 8)

	// reuse hashes from cardinality estimation if available
	if hashes == nil {
		// pre-alloc a hash slice
		hashes = arena.AllocUint64(b.Len())[:b.Len()]
		switch b.Type() {
		case block.BlockInt64, block.BlockUint64, block.BlockFloat64:
			// we write uint64 data in little endian order into the filter,
			// so all 8 byte numeric types look the same (float64 uses FloatBits == uint64)
			hashes = hash.Vec64(b.Uint64().Slice(), hashes)

		case block.BlockInt32, block.BlockUint32, block.BlockFloat32:
			// we write uint32 data in little endian order into the filter,
			// so all 4 byte numeric types look the same (float32 uses FloatBits == uint32)
			hashes = hash.Vec32(b.Uint32().Slice(), hashes)

		case block.BlockInt16, block.BlockUint16:
			// we write uint16 data in little endian order into the filter,
			// so all 2 byte numeric types look the
			hashes = hash.Vec16(b.Uint16().Slice(), hashes)

		case block.BlockInt8, block.BlockUint8:
			hashes = hash.Vec8(b.Uint8().Slice(), hashes)

		case block.BlockInt256:
			// write individual elements (no optimization exists)
			for i, v := range b.Int256().Iterator() {
				hashes[i] = hash.Hash(v.Bytes())
			}

		case block.BlockInt128:
			// write individual elements (no optimization exists)
			for i, v := range b.Int128().Iterator() {
				hashes[i] = hash.Hash(v.Bytes())
			}

		case block.BlockBytes:
			// write only unique elements (post-dedup optimization this avoids
			// calculating hashes for duplicates)
			for i, v := range b.Bytes().Iterator() {
				hashes[i] = hash.Hash(v)
			}

		default:
			// BlockBool and unknown/future types have no filter
			return nil
		}
	}

	// add values to filter
	flt.Add(hashes...)
	arena.Free(hashes)

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

func BuildFuseFilter[T uint8 | uint16](b *block.Block) (*fuse.BinaryFuse[T], error) {
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
		u64 = arena.AllocUint64(b.Len())
		defer arena.Free(u64)
		for i, v := range b.Int256().Iterator() {
			u64[i] = hash.Hash(v.Bytes())
		}

	case block.BlockInt128:
		// write individual elements (no optimization exists)
		u64 = arena.AllocUint64(b.Len())
		defer arena.Free(u64)
		for i, v := range b.Int128().Iterator() {
			u64[i] = hash.Hash(v.Bytes())
		}

	case block.BlockBytes:
		// write all strings
		u64 = arena.AllocUint64(b.Len())
		defer arena.Free(u64)
		for i, v := range b.Bytes().Iterator() {
			u64[i] = hash.Hash(v)
		}

	default:
		// BlockFloat32/64, BlockBool and unknown/future types have no filter
		return nil, schema.ErrInvalidValueType
	}

	// need unique values for filter construction
	u64 = slicex.Unique(u64)
	return fuse.Build[T](u64)
}
