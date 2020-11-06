// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition lists

package pack

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

const (
	filterThreshold = 2 // use hash map for IN conds with at least N entries
)

// Design: AND/OR & nested conditions (requires Query API change)
//
// type ConditionListMode int
//
// const (
// 	ConditionListModeAnd ConditionListMode = iota
// 	ConditionListModeOr  ConditionListMode = iota
// )
//
// type ConditionNode struct {
// 	Cond   *Condition
// 	Mode   ConditionListMode
// 	Nodes  ConditionNodeList
// }
//
// type ConditionNodeList []ConditionNode
//
// // returns the decision tree size (including sub-conditions)
// func (c ConditionNode) Weight() int {
// 	w := 0
// 	if c.Cond != nil {
// 		w++
// 	}
// 	for i, _ := range c.Nodes {
// 		w += c.Children[i].Weight()
// 	}
// 	return w
// }
//
// // returns the subtree execution cost based on the number of packs
// // that needs to be visited
// func (c ConditionNode) Cost() int {
// 	w := 0
// 	if c.Cond != nil {
// 		w++
// 	}
// 	for i, _ := range c.Nodes {
// 		w += c.Children[i].Weight()
// 	}
// 	return w
// }

type Condition struct {
	Field    Field       // evaluated table field
	Mode     FilterMode  // eq|ne|gt|gte|lt|lte|in|nin|re
	Raw      string      // string value when parsed from a query string
	Value    interface{} // typed value
	From     interface{} // typed value for between
	To       interface{} // typed value for between
	IsSorted bool        // in condition slice is already pre-sorted

	// internal data and statistics
	processed    bool                // condition has been processed already
	hashmap      map[uint64]int      // compiled hashmap for byte/string set queries
	hashoverflow []hashvalue         // hash collision overflow list (one for all)
	int64map     map[int64]struct{}  // compiled int64 map for set membership
	uint64map    map[uint64]struct{} // compiled uint64 map for set membership
	numValues    int                 // number of values when Value is a slice
}

type hashvalue struct {
	hash uint64
	pos  int
}

// returns the number of values to compare 1 (other), 2 (RANGE), many (IN)
func (c Condition) NValues() int {
	return c.numValues
}

func (c Condition) Check() error {
	// check condition values are of correct type for field
	switch c.Mode {
	case FilterModeRange:
		// expects From and To to be set
		if c.From == nil || c.To == nil {
			return fmt.Errorf("range condition expects From and To values")
		}
		if err := c.Field.Type.CheckType(c.From); err != nil {
			return err
		}
		if err := c.Field.Type.CheckType(c.To); err != nil {
			return err
		}
	case FilterModeIn, FilterModeNotIn:
		// expects a slice of values
		if err := c.Field.Type.CheckSliceType(c.Value); err != nil {
			return err
		}
	case FilterModeRegexp:
		// expects string only
		if err := FieldTypeString.CheckType(c.Value); err != nil {
			return err
		}
	default:
		// c.Value is a simple value type
		if err := c.Field.Type.CheckType(c.Value); err != nil {
			return err
		}
	}
	return nil
}

func (c *Condition) Compile() {
	// set number of values
	if c.Value != nil {
		c.numValues++
	}
	if c.From != nil {
		c.numValues++
	}
	if c.To != nil {
		c.numValues++
	}

	// only supported for set membership
	switch c.Mode {
	case FilterModeIn, FilterModeNotIn:
	default:
		return
	}

	// sort original input slices (required for later checks in FieldType.In/InAt)
	switch c.Field.Type {
	case FieldTypeBytes:
		if slice := c.Value.([][]byte); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return bytes.Compare(slice[i], slice[j]) < 0
				})
				c.IsSorted = true
			}
		}
	case FieldTypeString:
		if slice := c.Value.([]string); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i] < slice[j]
				})
				c.IsSorted = true
			}
		}
	case FieldTypeDatetime:
		if slice := c.Value.([]time.Time); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i].Before(slice[j])
				})
				c.IsSorted = true
			}
		}
	case FieldTypeBoolean:
		if slice := c.Value.([]bool); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return !slice[i] && slice[j]
				})
				c.IsSorted = true
			}
		}
	case FieldTypeInt64:
		if slice := c.Value.([]int64); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i] < slice[j]
				})
				c.IsSorted = true
			}
		}
	case FieldTypeUint64:
		if slice := c.Value.([]uint64); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i] < slice[j]
				})
				c.IsSorted = true
			}
		}
	case FieldTypeFloat64:
		if slice := c.Value.([]float64); slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				sort.Slice(slice, func(i, j int) bool {
					return slice[i] < slice[j]
				})
				c.IsSorted = true
			}
		}
	}

	var vals [][]byte

	// hash maps are only supported for expensive types, other types
	// will use a standard go map (which uses hashing internally)
	switch c.Field.Type {
	case FieldTypeInt64:
		// use a map for integer lookups
		slice := c.Value.([]int64)
		c.int64map = make(map[int64]struct{}, len(slice))
		for _, v := range slice {
			c.int64map[v] = struct{}{}
		}
		return
	case FieldTypeUint64:
		// use a map for unsigned integer lookups unless the checked
		// slices are guaranteed to be sorted (such as primary keys)
		if c.Field.Flags&FlagPrimary == 0 {
			slice := c.Value.([]uint64)
			c.uint64map = make(map[uint64]struct{}, len(slice))
			for _, v := range slice {
				c.uint64map[v] = struct{}{}
			}
		}
		return
	case FieldTypeBytes:
		// require [][]byte slice as value type
		vals = c.Value.([][]byte)
		if vals == nil {
			return
		}
	case FieldTypeString:
		// convert to []byte for feeding the hash map below
		strs := c.Value.([]string)
		if strs == nil {
			return
		}
		vals = make([][]byte, len(strs))
		for i, v := range strs {
			vals[i] = []byte(v)
		}
	default:
		return
	}

	// below min size a hash map is more expensive than memcmp
	if len(vals) < filterThreshold {
		return
	}

	// create a hash map
	c.hashmap = make(map[uint64]int)
	for i, v := range vals {
		sum := xxhash.Sum64(v)
		// ensure we're collision free
		if mapval, ok := c.hashmap[sum]; !ok {
			c.hashmap[sum] = i
		} else {
			// move current value and new value into overflow list
			if mapval != 0xFFFFFFFF {
				log.Warnf("pack: condition hash collision %0x / %0x == %0x", v, vals[mapval], sum)
				// there's already an overflow value
				c.hashoverflow = append(c.hashoverflow, hashvalue{
					hash: sum,
					pos:  mapval,
				})
			} else {
				log.Warnf("pack: double condition hash collision %0x == %0x", v, sum)
			}
			// add the current value to overflow
			c.hashoverflow = append(c.hashoverflow, hashvalue{
				hash: sum,
				pos:  i,
			})
			// signal this hash map entry has overflow entries
			c.hashmap[sum] = 0xFFFFFFFF
		}
	}
	// log.Debugf("query: compiled hash map condition with size %s for %d values",
	// 	util.ByteSize(len(vals)*12), len(vals))
}

// match package min/max values against the condition
func (c Condition) MaybeMatchPack(head PackageHeader) bool {
	min, max := head.BlockHeaders[c.Field.Index].MinValue, head.BlockHeaders[c.Field.Index].MaxValue
	switch c.Mode {
	case FilterModeEqual:
		// condition value is within range
		return c.Field.Type.Lte(min, c.Value) && c.Field.Type.Gte(max, c.Value)
	case FilterModeNotEqual:
		// condition is either strictly smaller or strictly larger
		return c.Field.Type.Lt(min, c.Value) || c.Field.Type.Gt(max, c.Value)
	case FilterModeRange:
		// check if pack min-max range overlaps c.From-c.To range
		return !(c.Field.Type.Lt(max, c.From) || c.Field.Type.Gt(min, c.To))
	case FilterModeIn:
		// check if any of the IN condition values fall into the pack's min and max range
		return c.Field.Type.InBetween(c.Value, min, max) // c.Value is a slice
	case FilterModeNotIn:
		return true // we don't know here, so full scan is required
	case FilterModeRegexp:
		return true // we don't know, so full scan is required
	case FilterModeGt:
		// block min OR max is > condition value
		return c.Field.Type.Gt(min, c.Value) || c.Field.Type.Gt(max, c.Value)
	case FilterModeGte:
		// block min OR max is >= condition value
		return c.Field.Type.Gte(min, c.Value) || c.Field.Type.Gte(max, c.Value)
	case FilterModeLt:
		// block min OR max is < condition value
		return c.Field.Type.Lt(min, c.Value) || c.Field.Type.Lt(max, c.Value)
	case FilterModeLte:
		// block min OR max is <= condition value
		return c.Field.Type.Lte(min, c.Value) || c.Field.Type.Lte(max, c.Value)
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
			return fmt.Sprintf("%s %s [%d values]", c.Field.Name, c.Mode.Op(), size)
		} else {
			return fmt.Sprintf("%s %s [%v]", c.Field.Name, c.Mode.Op(), c.Field.Type.ToString(c.Value))
		}
	default:
		return fmt.Sprintf("%s %s %s [%s]", c.Field.Name, c.Mode.Op(), util.ToString(c.Value), c.Raw)
	}
}

// parse conditions from query string
// col_name.{gt|gte|lt|lte|ne|in|nin|rg|re}=value
func ParseCondition(key, val string, fields FieldList) (Condition, error) {
	var (
		c    Condition
		f, m string
		err  error
	)
	if ff := strings.Split(key, "."); len(ff) == 2 {
		f, m = ff[0], ff[1]
	} else {
		f = ff[0]
		m = "eq"
	}
	c.Field = fields.Find(f)
	if !c.Field.IsValid() {
		return c, fmt.Errorf("unknown column '%s'", f)
	}
	c.Mode = ParseFilterMode(m)
	if !c.Mode.IsValid() {
		return c, fmt.Errorf("invalid filter mode '%s'", m)
	}
	c.Raw = val
	switch c.Mode {
	case FilterModeRange:
		vv := strings.Split(val, ",")
		if len(vv) != 2 {
			return c, fmt.Errorf("range conditions require exactly two arguments")
		}
		c.From, err = c.Field.Type.ParseAs(vv[0])
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
		c.To, err = c.Field.Type.ParseAs(vv[1])
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	case FilterModeIn, FilterModeNotIn:
		c.Value, err = c.Field.Type.ParseSliceAs(val)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	default:
		c.Value, err = c.Field.Type.ParseAs(val)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	}
	return c, nil
}

type ConditionList []Condition

// may otimize (reduce/merge/replace) conditions in the future
func (l *ConditionList) Compile(t *Table) error {
	for i, _ := range *l {
		if err := (*l)[i].Check(); err != nil {
			return fmt.Errorf("cond %d on table field '%s.%s': %v", i, t.name, (*l)[i].Field.Name, err)
		}
		(*l)[i].Compile()
	}
	return nil
}

// returns unique list of fields
func (l ConditionList) Fields() FieldList {
	if len(l) == 0 {
		return nil
	}
	fl := make(FieldList, 0, len(l))
	for i, _ := range l {
		// add any direct fields
		fl = fl.AddUnique(l[i].Field)
		// add child fields recursively
		// fl.AddUnique(l[i].Children.Fields()...)
	}
	return fl
}

func (l ConditionList) MaybeMatchPack(head PackageHeader) bool {
	if head.NValues == 0 {
		return false
	}
	// always match empty condition list
	if len(l) == 0 {
		return true
	}
	for i, _ := range l {
		// this is equivalent to an AND between all conditions in list
		if l[i].MaybeMatchPack(head) {
			continue
		}
		return false
	}
	return true
}

// return a bit vector containing matching positions in the pack
// TODO: consider parallel matches to check multiple conditions, then merge bitsets
func (l ConditionList) MatchPack(pkg *Package) *vec.BitSet {
	// always match empty condition list
	if len(l) == 0 || pkg.Len() == 0 {
		allOnes := vec.NewBitSet(pkg.Len())
		allOnes.One()
		return allOnes
	}
	// match conditions and merge bit vectors
	// stop early when result contains all zeros (assuming AND
	// relation between all conditions)
	var bits *vec.BitSet
	for _, c := range l {
		b := c.MatchPack(pkg)
		if bits == nil {
			if b.Count() == 0 {
				return b
			}
			bits = b
			continue
		}
		// early stop on empty match
		if b.Count() == 0 {
			bits.Close()
			return b
		}
		bits.And(b)
		b.Close()
	}
	return bits
}

func (c Condition) MatchPack(pkg *Package) *vec.BitSet {
	bits := vec.NewBitSet(pkg.Len())
	slice, _ := pkg.Column(c.Field.Index)
	switch c.Mode {
	case FilterModeEqual:
		return c.Field.Type.EqualSlice(slice, c.Value, bits)
	case FilterModeNotEqual:
		return c.Field.Type.NotEqualSlice(slice, c.Value, bits)
	case FilterModeGt:
		return c.Field.Type.GtSlice(slice, c.Value, bits)
	case FilterModeGte:
		return c.Field.Type.GteSlice(slice, c.Value, bits)
	case FilterModeLt:
		return c.Field.Type.LtSlice(slice, c.Value, bits)
	case FilterModeLte:
		return c.Field.Type.LteSlice(slice, c.Value, bits)
	case FilterModeRange:
		return c.Field.Type.BetweenSlice(slice, c.From, c.To, bits)
	case FilterModeRegexp:
		return c.Field.Type.RegexpSlice(slice, c.Value.(string), bits)
	case FilterModeIn:
		// unlink with the other types we use the compiled maps/filters
		// and execute the matching loop here rather than using vectorized
		// type functions
		// type check was already performed in compile stage
		switch c.Field.Type {
		case FieldTypeInt64:
			for i, v := range slice.([]int64) {
				if _, ok := c.int64map[v]; ok {
					bits.Set(i)
				}
			}

		case FieldTypeUint64:
			// optimization for primary key fields: where pk columns
			// are sorted, so we can employ a more space/time efficient
			// matching algorithm here
			pk := slice.([]uint64)
			in := c.Value.([]uint64)
			if c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
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
						bits.Set(p)
						i++
					}
				}
			} else {
				for i, v := range pk {
					if _, ok := c.uint64map[v]; ok {
						bits.Set(i)
					}
				}
			}

		// strings and bytes use a hash map; any negative response means
		// val is NOT part of the set and can be rejected; any positive
		// response may be a false positive with very low probability
		// due to hash collision; we use a global overflow list to catch
		// this case (i.e. the list contains all colliding values)
		case FieldTypeBytes:
			vals := c.Value.([][]byte)
			for i, v := range slice.([][]byte) {
				if c.hashmap != nil {
					sum := xxhash.Sum64(v)
					if pos, ok := c.hashmap[sum]; ok {
						if pos != 0xFFFFFFFF {
							// compare IN slice value at pos against value
							// to ensure we're collision free
							if bytes.Compare(v, vals[pos]) == 0 {
								bits.Set(i)
							}
						} else {
							// scan overflow list
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if bytes.Compare(v, vals[oflow.pos]) != 0 {
									continue
								}
								bits.Set(i)
								break
							}
						}
					}
				} else {
					// without hash map, resort to type-based comparison
					if c.Field.Type.In(v, c.Value) {
						bits.Set(i)
					}
				}
			}

		case FieldTypeString:
			strs := c.Value.([]string)
			for i, v := range slice.([]string) {
				if c.hashmap != nil {
					sum := xxhash.Sum64([]byte(v))
					if pos, ok := c.hashmap[sum]; ok {
						if pos != 0xFFFFFFFF {
							// compare IN slice value at pos against buf
							// to ensure we're collision free
							if strings.Compare(v, strs[pos]) == 0 {
								bits.Set(i)
							}
						} else {
							// scan overflow list
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if strings.Compare(v, strs[oflow.pos]) != 0 {
									continue
								}
								bits.Set(i)
								break
							}
						}
					}
				} else {
					// without hash map, resort to type-based comparison
					if c.Field.Type.In(v, c.Value) {
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
		case FieldTypeInt64:
			for i, v := range slice.([]int64) {
				if _, ok := c.int64map[v]; !ok {
					bits.Set(i)
				}
			}

		case FieldTypeUint64:
			// optimization for primary key fields: where pk columns
			// are sorted, so we can employ a more space/time efficient
			// matching algorithm here; Note that in contrast to IN
			// conditions we negate the bitset in the end
			pk := slice.([]uint64)
			in := c.Value.([]uint64)
			if c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
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
						bits.Set(p)
						i++
					}
				}
				// negate the positive match result from above
				bits.Neg()
			} else {
				// check each slice element against the map
				for i, v := range pk {
					if _, ok := c.uint64map[v]; !ok {
						bits.Set(i)
					}
				}
			}

		// strings and bytes use a hash map; any negative response means
		// val is NOT part of the set and can be rejected; any positive
		// response may be a false positive with very low probability
		// due to hash collision; we use a global overflow list to catch
		// this case (i.e. the list contains all colliding values)
		case FieldTypeBytes:
			vals := c.Value.([][]byte)
			for i, v := range slice.([][]byte) {
				if c.hashmap != nil {
					sum := xxhash.Sum64(v)
					if pos, ok := c.hashmap[sum]; !ok {
						bits.Set(i)
					} else {
						// may still be a false positive due to hash collision
						if pos != 0xFFFFFFFF {
							// compare IN slice value at pos against buf
							// to ensure we're collision free
							if bytes.Compare(v, vals[pos]) != 0 {
								bits.Set(i)
							}
						} else {
							// scan overflow list, must use exhaustive search
							var found bool
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if bytes.Compare(v, vals[oflow.pos]) == 0 {
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
			for i, v := range slice.([]string) {
				if c.hashmap != nil {
					sum := xxhash.Sum64([]byte(v))
					if pos, ok := c.hashmap[sum]; !ok {
						bits.Set(i)
					} else {
						// may still be a false positive due to hash collision
						if pos != 0xFFFFFFFF {
							// compare IN slice value at pos against buf
							// to ensure we're collision free
							if strings.Compare(v, strs[pos]) != 0 {
								bits.Set(i)
							}
						} else {
							// scan overflow list, must use exhaustive search
							var found bool
							for _, oflow := range c.hashoverflow {
								if oflow.hash != sum {
									continue
								}
								if strings.Compare(v, strs[oflow.pos]) == 0 {
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
					if c.Field.Type.In(v, c.Value) {
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

// DEPRECATED, only used for testcases and benchmarks

// match a single value usually from a pack vector against the condition
func (c Condition) Match(val interface{}) bool {
	switch c.Mode {
	case FilterModeEqual:
		return c.Field.Type.Equal(val, c.Value)
	case FilterModeNotEqual:
		return !c.Field.Type.Equal(val, c.Value)
	case FilterModeRange:
		return c.Field.Type.Between(val, c.From, c.To)
	case FilterModeIn:
		// type check on val was already performed in compile stage
		switch c.Field.Type {
		case FieldTypeInt64:
			_, ok := c.int64map[val.(int64)]
			return ok
		case FieldTypeUint64:
			_, ok := c.uint64map[val.(uint64)]
			return ok
		}

		// strings and bytes use bloom filter or hash map
		// any negative response means val is NOT part of the set and can
		// be rejected, any positive response may be a false positive with
		// low probability
		var buf []byte
		if c.Field.Type == FieldTypeBytes {
			buf = val.([]byte)
		} else if c.Field.Type == FieldTypeString {
			buf = []byte(val.(string))
		}
		if buf != nil && c.hashmap != nil {
			if _, ok := c.hashmap[xxhash.Sum64(buf)]; !ok {
				return false
			}
		}
		// any other value is delegated (Note: due to false positives also
		// byte and string conditions are checked again)
		return c.Field.Type.In(val, c.Value) // c.Value is a slice
	case FilterModeNotIn:
		// type check on val was already performed in compile stage
		switch c.Field.Type {
		case FieldTypeInt64:
			_, ok := c.int64map[val.(int64)]
			return !ok
		case FieldTypeUint64:
			_, ok := c.uint64map[val.(uint64)]
			return !ok
		}

		// strings and bytes use bloom filter or hash map
		// any negative response means val is NOT part of the set and can
		// be rejected right away, any positive response may be a false
		// positive with low probability
		var buf []byte
		if c.Field.Type == FieldTypeBytes {
			buf = val.([]byte)
		} else if c.Field.Type == FieldTypeString {
			buf = []byte(val.(string))
		}
		if buf != nil && c.hashmap != nil {
			if _, ok := c.hashmap[xxhash.Sum64(buf)]; !ok {
				return true
			}
		}
		// any other value is delegated (Note: due to false positives also
		// byte and string conditions are checked again)
		return !c.Field.Type.In(val, c.Value) // c.Value is a slice
	case FilterModeRegexp:
		return c.Field.Type.Regexp(val, c.Value.(string)) // c.Value is regexp string
	case FilterModeGt:
		return c.Field.Type.Gt(val, c.Value)
	case FilterModeGte:
		return c.Field.Type.Gte(val, c.Value)
	case FilterModeLt:
		return c.Field.Type.Lt(val, c.Value)
	case FilterModeLte:
		return c.Field.Type.Lte(val, c.Value)
	default:
		return false
	}
}

// TODO: support more than a simple AND between conditions
func (l ConditionList) MatchAt(pkg *Package, pos int) bool {
	if len(l) == 0 {
		return true
	}
	if pkg.Len() <= pos {
		return false
	}
	for _, c := range l {
		if !c.MatchAt(pkg, pos) {
			return false
		}
	}
	return true
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
		case FieldTypeInt64:
			val, _ := pkg.Int64At(index, pos)
			_, ok := c.int64map[val]
			return ok
		case FieldTypeUint64:
			val, _ := pkg.Uint64At(index, pos)
			_, ok := c.uint64map[val]
			return ok
		}

		// strings and bytes use bloom filter or hash map
		// any negative response means val is NOT part of the set and can
		// be rejected, any positive response may be a false positive with
		// low probability
		// type check on val was already performed in compile stage
		var buf []byte
		if c.Field.Type == FieldTypeBytes {
			buf, _ = pkg.BytesAt(index, pos)
		} else if c.Field.Type == FieldTypeString {
			str, _ := pkg.StringAt(index, pos)
			buf = []byte(str)
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
		case FieldTypeInt64:
			val, _ := pkg.Int64At(index, pos)
			_, ok := c.int64map[val]
			return !ok
		case FieldTypeUint64:
			val, _ := pkg.Uint64At(index, pos)
			_, ok := c.uint64map[val]
			return !ok
		}

		// strings and bytes use bloom filter or hash map
		// any negative response means val is NOT part of the set and can
		// be rejected, any positive response may be a false positive with
		// low probability
		// type check on val was already performed in compile stage
		var buf []byte
		if c.Field.Type == FieldTypeBytes {
			buf, _ = pkg.BytesAt(index, pos)
		} else if c.Field.Type == FieldTypeString {
			str, _ := pkg.StringAt(index, pos)
			buf = []byte(str)
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
