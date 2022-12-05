// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition

package pack

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/util"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/vec"
)

const (
	COND_OR  = true
	COND_AND = false
)

type Condition struct {
	Field    *Field      // evaluated table field
	Mode     FilterMode  // eq|ne|gt|gte|lt|lte|in|nin|re
	Raw      string      // string value when parsed from a query string
	Value    interface{} // typed value
	From     interface{} // typed value for range queries
	To       interface{} // typed value for range queries
	IsSorted bool        // IN/NIN condition slice is already pre-sorted

	// internal data and statistics
	processed    bool                    // condition has been processed already
	nomatch      bool                    // condition is empty (used on index matches)
	hashmap      map[uint64]int          // compiled hashmap for byte/string set queries
	hashoverflow []hashvalue             // hash collision overflow list (one for all)
	int256map    map[vec.Int256]struct{} // compiled int256 map for set membership
	int128map    map[vec.Int128]struct{} // compiled int128 map for set membership
	int64map     map[int64]struct{}      // compiled int64 map for set membership
	int32map     map[int32]struct{}      // compiled int32 map for set membership
	int16map     map[int16]struct{}      // compiled int16 map for set membership
	int8map      map[int8]struct{}       // compiled int8 map for set membership
	uint64map    map[uint64]struct{}     // compiled uint64 map for set membership
	uint32map    map[uint32]struct{}     // compiled uint32 map for set membership
	uint16map    map[uint16]struct{}     // compiled uint16 map for set membership
	uint8map     map[uint8]struct{}      // compiled uint8 map for set membership
	numValues    int                     // number of values when Value is a slice
	bloomHashes  [][2]uint32             // opt bloom hash value(s) if field has bloom flag
}

// returns the number of values to compare 1 (other), 2 (RANGE), many (IN)
func (c Condition) NValues() int {
	return c.numValues
}

// match package min/max values against the condition
// Note: min/max are raw storage values (i.e. for decimals, they are scaled integers)
func (c Condition) MaybeMatchPack(info PackInfo) bool {
	idx := c.Field.Index
	min := info.Blocks[idx].MinValue
	max := info.Blocks[idx].MaxValue
	filter := info.Blocks[idx].Bloom
	bitmap := info.Blocks[idx].Bitmap
	scale := c.Field.Scale
	typ := c.Field.Type
	// decimals only: convert storage type used in block info to field type
	switch typ {
	case FieldTypeDecimal32:
		min = decimal.NewDecimal32(min.(int32), scale)
		max = decimal.NewDecimal32(max.(int32), scale)
	case FieldTypeDecimal64:
		min = decimal.NewDecimal64(min.(int64), scale)
		max = decimal.NewDecimal64(max.(int64), scale)
	case FieldTypeDecimal128:
		min = decimal.NewDecimal128(min.(vec.Int128), scale)
		max = decimal.NewDecimal128(max.(vec.Int128), scale)
	case FieldTypeDecimal256:
		min = decimal.NewDecimal256(min.(vec.Int256), scale)
		max = decimal.NewDecimal256(max.(vec.Int256), scale)
	}
	// compare pack header
	switch c.Mode {
	case FilterModeEqual:
		// condition value is within range
		res := typ.Between(c.Value, min, max)
		if res && filter != nil {
			return filter.ContainsHash(c.bloomHashes[0])
		}
		if res && bitmap != nil {
			return bitmap.IsSet(c.Value.(int))
		}
		return res
	case FilterModeNotEqual:
		return true // we don't know, so full scan is required
	case FilterModeRange:
		// check if pack min-max range overlaps c.From-c.To range
		return !(typ.Lt(max, c.From) || typ.Gt(min, c.To))
	case FilterModeIn:
		// check if any of the IN condition values fall into the pack's min and max range
		res := typ.InBetween(c.Value, min, max) // c.Value is a slice
		if res && filter != nil {
			return filter.ContainsAnyHash(c.bloomHashes)
		}
		if res && bitmap != nil {
			return bitmap.IsSetAny(c.Value.([]int))
		}
		return res
	case FilterModeNotIn:
		return true // we don't know here, so full scan is required
	case FilterModeRegexp:
		return true // we don't know, so full scan is required
	case FilterModeGt:
		// min OR max is > condition value
		return typ.Gt(min, c.Value) || typ.Gt(max, c.Value)
	case FilterModeGte:
		// min OR max is >= condition value
		return typ.Gte(min, c.Value) || typ.Gte(max, c.Value)
	case FilterModeLt:
		// min OR max is < condition value
		return typ.Lt(min, c.Value) || typ.Lt(max, c.Value)
	case FilterModeLte:
		// min OR max is <= condition value
		return typ.Lte(min, c.Value) || typ.Lte(max, c.Value)
	default:
		return false
	}
}

func (c Condition) String() string {
	switch c.Mode {
	case FilterModeRange:
		return fmt.Sprintf("%s %s [%s, %s]", c.Field.Name, c.Mode.Op(),
			util.ToString(c.From), util.ToString(c.To))
	case FilterModeIn, FilterModeNotIn:
		size := c.numValues
		if size == 0 {
			size = reflect.ValueOf(c.Value).Len()
		}
		if size > 16 {
			return fmt.Sprintf("%s %s [%d values] sorted=%t", c.Field.Name, c.Mode.Op(), size, c.IsSorted)
		} else {
			return fmt.Sprintf("%s %s sorted=%t %v", c.Field.Name, c.Mode.Op(), c.IsSorted, c.Field.Type.SliceToString(c.Value, c.Field))
		}
	default:
		s := fmt.Sprintf("%s %s %s", c.Field.Name, c.Mode.Op(), util.ToString(c.Value))
		if len(c.Raw) > 0 {
			s += " [" + c.Raw + "]"
		}
		return s
	}
}

// MatchPack matches all elements in package pkg against the defined condition
// and returns a bitset of the same length as the package with bits set to true
// where the match is successful.
//
// This implementation uses low level block vectors to efficiently execute
// vectorized checks with custom assembly-optimized routines.
func (c Condition) MatchPack(pkg *Package, mask *vec.Bitset) *vec.Bitset {
	bits := vec.NewBitset(pkg.Len())
	block, _ := pkg.Block(c.Field.Index)
	switch c.Mode {
	case FilterModeEqual:
		return c.Field.Type.EqualBlock(block, c.Value, bits, mask)
	case FilterModeNotEqual:
		return c.Field.Type.NotEqualBlock(block, c.Value, bits, mask)
	case FilterModeGt:
		return c.Field.Type.GtBlock(block, c.Value, bits, mask)
	case FilterModeGte:
		return c.Field.Type.GteBlock(block, c.Value, bits, mask)
	case FilterModeLt:
		return c.Field.Type.LtBlock(block, c.Value, bits, mask)
	case FilterModeLte:
		return c.Field.Type.LteBlock(block, c.Value, bits, mask)
	case FilterModeRange:
		return c.Field.Type.BetweenBlock(block, c.From, c.To, bits, mask)
	case FilterModeRegexp:
		return c.Field.Type.RegexpBlock(block, c.Value.(string), bits, mask)
	case FilterModeIn:
		// unlike on other conditions we run matches against a standard map
		// rather than using vectorized type functions
		// type check was already performed in compile stage
		switch c.Field.Type {
		case FieldTypeInt256, FieldTypeDecimal256:
			for i := 0; i < block.Int256.Len(); i++ {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int256map[block.Int256.Elem(i)]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt128, FieldTypeDecimal128:
			for i := 0; i < block.Int128.Len(); i++ {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int128map[block.Int128.Elem(i)]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt64, FieldTypeDecimal64, FieldTypeDatetime:
			for i, v := range block.Int64 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int64map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt32:
			for i, v := range block.Int32 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int32map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt16:
			for i, v := range block.Int16 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int16map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt8:
			for i, v := range block.Int8 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int8map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeUint64:
			// optimization for primary key fields: where pk columns
			// are sorted, so we can employ a more space/time efficient
			// matching algorithm here
			pk := block.Uint64
			in := c.Value.([]uint64)
			// journal pack is unsorted, so we fall back to using a map
			if pkg.IsJournal() && c.uint64map == nil {
				c.uint64map = make(map[uint64]struct{}, len(in))
				for _, v := range in {
					c.uint64map[v] = struct{}{}
				}
			}
			if !pkg.IsJournal() && c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
				maxin := in[len(in)-1]
				maxpk := pk[len(pk)-1]
				for i, p, il, pl := 0, 0, len(in), len(pk); i < il && p < pl; {
					if pk[p] > maxin || maxpk < in[i] {
						// no more matches in this pack
						break
					}
					for p < pl && pk[p] < in[i] {
						p++
					}
					if p == pl {
						break
					}
					for i < il && pk[p] > in[i] {
						i++
					}
					if i == il {
						break
					}
					if pk[p] == in[i] {
						// blend masked values
						if mask == nil || mask.IsSet(p) {
							bits.Set(p)
						}
						i++
					}
				}
			} else {
				for i, v := range pk {
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
					if _, ok := c.uint64map[v]; ok {
						bits.Set(i)
					}
				}
			}

		case FieldTypeUint32:
			for i, v := range block.Uint32 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.uint32map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeUint16:
			for i, v := range block.Uint16 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.uint16map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeUint8:
			for i, v := range block.Uint8 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.uint8map[v]; ok {
					bits.Set(i)
				}
			}

		// strings and bytes use a hash map; any negative response means
		// val is NOT part of the set and can be rejected; any positive
		// response may be a false positive with very low probability
		// due to hash collision; we use a global overflow list to catch
		// this case (i.e. the list contains all colliding values)
		case FieldTypeBytes:
			vals := c.Value.([][]byte)
			if c.hashmap != nil {
				for i := 0; i < block.Bytes.Len(); i++ {
					v := block.Bytes.Elem(i)
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
					sum := xxhash.Sum64(v)
					if pos, ok := c.hashmap[sum]; ok {
						if pos != 0xFFFFFFFF {
							// compare IN slice value at pos against value
							// to ensure we're collision free
							if bytes.Equal(v, vals[pos]) {
								bits.Set(i)
							}
						} else {
							// scan overflow list
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if !bytes.Equal(v, vals[oflow.pos]) {
									continue
								}
								bits.Set(i)
								break
							}
						}
					}
				}
			} else {
				for i := 0; i < block.Bytes.Len(); i++ {
					v := block.Bytes.Elem(i)
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
					// without hash map, resort to type-based comparison
					if c.Field.Type.In(v, c.Value) {
						bits.Set(i)
					}
				}
			}

		case FieldTypeString:
			strs := c.Value.([]string)
			if c.hashmap != nil {
				for i := 0; i < block.Bytes.Len(); i++ {
					v := block.Bytes.Elem(i)
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
					sum := xxhash.Sum64(v)
					if pos, ok := c.hashmap[sum]; ok {
						vs := compress.UnsafeGetString(v)
						if pos != 0xFFFFFFFF {
							// compare IN slice value at pos against buf
							// to ensure we're collision free
							if strings.Compare(vs, strs[pos]) == 0 {
								bits.Set(i)
							}
						} else {
							// scan overflow list
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if strings.Compare(vs, strs[oflow.pos]) != 0 {
									continue
								}
								bits.Set(i)
								break
							}
						}
					}
				}
			} else {
				for i := 0; i < block.Bytes.Len(); i++ {
					v := block.Bytes.Elem(i)
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
					// without hash map, resort to type-based comparison
					if c.Field.Type.In(compress.UnsafeGetString(v), c.Value) {
						bits.Set(i)
					}
				}
			}
		}

		return bits

	case FilterModeNotIn:
		// unlike with the other types we use the compiled maps and run
		// the matching loop here rather than using vectorized functions
		//
		// type check was already performed in compile stage
		switch c.Field.Type {
		case FieldTypeInt256, FieldTypeDecimal256:
			for i := 0; i < block.Int256.Len(); i++ {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int256map[block.Int256.Elem(i)]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt128, FieldTypeDecimal128:
			for i := 0; i < block.Int128.Len(); i++ {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int128map[block.Int128.Elem(i)]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt64, FieldTypeDecimal64, FieldTypeDatetime:
			for i, v := range block.Int64 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int64map[v]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt32, FieldTypeDecimal32:
			for i, v := range block.Int32 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int32map[v]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt16:
			for i, v := range block.Int16 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int16map[v]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt8:
			for i, v := range block.Int8 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int8map[v]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeUint64:
			// optimization for primary key fields: where pk columns
			// are sorted, so we can employ a more space/time efficient
			// matching algorithm here; Note that in contrast to IN
			// conditions we negate the bitset in the end
			pk := block.Uint64
			in := c.Value.([]uint64)
			// journal pack is unsorted, so we fall back to using a map
			if pkg.IsJournal() && c.uint64map == nil {
				c.uint64map = make(map[uint64]struct{}, len(in))
				for _, v := range in {
					c.uint64map[v] = struct{}{}
				}
			}
			if !pkg.IsJournal() && c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
				maxin := in[len(in)-1]
				maxpk := pk[len(pk)-1]
				for i, p, il, pl := 0, 0, len(in), len(pk); i < il && p < pl; {
					if pk[p] > maxin || maxpk < in[i] {
						// no more matches in this pack
						break
					}
					for p < pl && pk[p] < in[i] {
						p++
					}
					if p == pl {
						break
					}
					for i < il && pk[p] > in[i] {
						i++
					}
					if i == il {
						break
					}
					if pk[p] == in[i] {
						// ignore mask
						bits.Set(p)
						i++
					}
				}
				// negate the positive match result from above
				bits.Neg()
			} else {
				// check each slice element against the map
				for i, v := range pk {
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
					if _, ok := c.uint64map[v]; !ok {
						bits.Set(i)
					}
				}
			}
		case FieldTypeUint32:
			for i, v := range block.Uint32 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.uint32map[v]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeUint16:
			for i, v := range block.Uint16 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.uint16map[v]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeUint8:
			for i, v := range block.Uint8 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.uint8map[v]; !ok {
					bits.Set(i)
				}
			}

		// strings and bytes use a hash map; any negative response means
		// val is NOT part of the set and can be rejected; any positive
		// response may be a false positive with very low probability
		// due to hash collision; we use a global overflow list to catch
		// this case (i.e. the list contains all colliding values)
		case FieldTypeBytes:
			vals := c.Value.([][]byte)
			for i := 0; i < block.Bytes.Len(); i++ {
				v := block.Bytes.Elem(i)
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if c.hashmap != nil {
					sum := xxhash.Sum64(v)
					if pos, ok := c.hashmap[sum]; !ok {
						bits.Set(i)
					} else {
						// may still be a false positive due to hash collision
						if pos != 0xFFFFFFFF {
							// compare IN slice value at pos against buf
							// to ensure we're collision free
							if !bytes.Equal(v, vals[pos]) {
								bits.Set(i)
							}
						} else {
							// scan overflow list, must use exhaustive search
							var found bool
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if bytes.Equal(v, vals[oflow.pos]) {
									// may break early when found
									found = true
									break
								}
							}
							if !found {
								bits.Set(i)
							}
						}
					}
				} else {
					// without hash map, resort to type-based comparison
					if !c.Field.Type.In(v, c.Value) {
						bits.Set(i)
					}
				}
			}

		case FieldTypeString:
			strs := c.Value.([]string)
			for i := 0; i < block.Bytes.Len(); i++ {
				v := block.Bytes.Elem(i)
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if c.hashmap != nil {
					sum := xxhash.Sum64(v)
					if pos, ok := c.hashmap[sum]; !ok {
						bits.Set(i)
					} else {
						vs := compress.UnsafeGetString(v)
						// may still be a false positive due to hash collision
						if pos != 0xFFFFFFFF {
							// compare IN slice value at pos against buf
							// to ensure we're collision free
							if strings.Compare(vs, strs[pos]) != 0 {
								bits.Set(i)
							}
						} else {
							// scan overflow list, must use exhaustive search
							var found bool
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if strings.Compare(vs, strs[oflow.pos]) == 0 {
									// may break early when found
									found = true
									break
								}
							}
							if !found {
								bits.Set(i)
							}
						}
					}
				} else {
					// without hash map, resort to type-based comparison
					if !c.Field.Type.In(compress.UnsafeGetString(v), c.Value) {
						bits.Set(i)
					}
				}
			}
		}
		return bits
	default:
		return bits
	}
}

func (c Condition) MatchAt(pkg *Package, pos int) bool {
	index := c.Field.Index
	switch c.Mode {
	case FilterModeEqual:
		return c.Field.Type.EqualAt(pkg, index, pos, c.Value)
	case FilterModeNotEqual:
		return !c.Field.Type.EqualAt(pkg, index, pos, c.Value)
	case FilterModeRange:
		return c.Field.Type.BetweenAt(pkg, index, pos, c.From, c.To)
	case FilterModeIn:
		// type check was already performed in compile stage
		switch c.Field.Type {
		case FieldTypeInt256, FieldTypeDecimal256:
			val, _ := pkg.Int256At(index, pos)
			_, ok := c.int256map[val]
			return ok
		case FieldTypeInt128, FieldTypeDecimal128:
			val, _ := pkg.Int128At(index, pos)
			_, ok := c.int128map[val]
			return ok
		case FieldTypeInt64, FieldTypeDecimal64, FieldTypeDatetime:
			val, _ := pkg.Int64At(index, pos)
			_, ok := c.int64map[val]
			return ok
		case FieldTypeInt32, FieldTypeDecimal32:
			val, _ := pkg.Int32At(index, pos)
			_, ok := c.int32map[val]
			return ok
		case FieldTypeInt16:
			val, _ := pkg.Int16At(index, pos)
			_, ok := c.int16map[val]
			return ok
		case FieldTypeInt8:
			val, _ := pkg.Int8At(index, pos)
			_, ok := c.int8map[val]
			return ok
		case FieldTypeUint64:
			val, _ := pkg.Uint64At(index, pos)
			_, ok := c.uint64map[val]
			return ok
		case FieldTypeUint32:
			val, _ := pkg.Uint32At(index, pos)
			_, ok := c.uint32map[val]
			return ok
		case FieldTypeUint16:
			val, _ := pkg.Uint16At(index, pos)
			_, ok := c.uint16map[val]
			return ok
		case FieldTypeUint8:
			val, _ := pkg.Uint8At(index, pos)
			_, ok := c.uint8map[val]
			return ok
		}

		// strings and bytes use bloom filter or hash map
		// any negative response means val is NOT part of the set and can
		// be rejected, any positive response may be a false positive with
		// low probability
		// type check on val was already performed in compile stage
		var buf []byte
		if c.Field.Type == FieldTypeBytes || c.Field.Type == FieldTypeString {
			buf, _ = pkg.BytesAt(index, pos)
		}
		if buf != nil && c.hashmap != nil {
			if _, ok := c.hashmap[xxhash.Sum64(buf)]; !ok {
				return false
			}
		}
		return c.Field.Type.InAt(pkg, index, pos, c.Value) // c.Value is a slice

	case FilterModeNotIn:
		// type check was already performed in compile stage
		switch c.Field.Type {
		case FieldTypeInt256, FieldTypeDecimal256:
			val, _ := pkg.Int256At(index, pos)
			_, ok := c.int256map[val]
			return !ok
		case FieldTypeInt128, FieldTypeDecimal128:
			val, _ := pkg.Int128At(index, pos)
			_, ok := c.int128map[val]
			return !ok
		case FieldTypeInt64, FieldTypeDecimal64, FieldTypeDatetime:
			val, _ := pkg.Int64At(index, pos)
			_, ok := c.int64map[val]
			return !ok
		case FieldTypeInt32, FieldTypeDecimal32:
			val, _ := pkg.Int32At(index, pos)
			_, ok := c.int32map[val]
			return !ok
		case FieldTypeInt16:
			val, _ := pkg.Int16At(index, pos)
			_, ok := c.int16map[val]
			return !ok
		case FieldTypeInt8:
			val, _ := pkg.Int8At(index, pos)
			_, ok := c.int8map[val]
			return !ok
		case FieldTypeUint64:
			val, _ := pkg.Uint64At(index, pos)
			_, ok := c.uint64map[val]
			return !ok
		case FieldTypeUint32:
			val, _ := pkg.Uint32At(index, pos)
			_, ok := c.uint32map[val]
			return !ok
		case FieldTypeUint16:
			val, _ := pkg.Uint16At(index, pos)
			_, ok := c.uint16map[val]
			return !ok
		case FieldTypeUint8:
			val, _ := pkg.Uint8At(index, pos)
			_, ok := c.uint8map[val]
			return !ok
		}

		// strings and bytes use bloom filter or hash map
		// any negative response means val is NOT part of the set and can
		// be rejected, any positive response may be a false positive with
		// low probability
		// type check on val was already performed in compile stage
		var buf []byte
		if c.Field.Type == FieldTypeBytes || c.Field.Type == FieldTypeString {
			buf, _ = pkg.BytesAt(index, pos)
		}
		if buf != nil && c.hashmap != nil {
			if _, ok := c.hashmap[xxhash.Sum64(buf)]; !ok {
				return true
			}
		}
		return !c.Field.Type.InAt(pkg, index, pos, c.Value) // c.Value is a slice

	case FilterModeRegexp:
		return c.Field.Type.RegexpAt(pkg, index, pos, c.Value.(string)) // c.Value is regexp string
	case FilterModeGt:
		return c.Field.Type.GtAt(pkg, index, pos, c.Value)
	case FilterModeGte:
		return c.Field.Type.GteAt(pkg, index, pos, c.Value)
	case FilterModeLt:
		return c.Field.Type.LtAt(pkg, index, pos, c.Value)
	case FilterModeLte:
		return c.Field.Type.LteAt(pkg, index, pos, c.Value)
	default:
		return false
	}
}
