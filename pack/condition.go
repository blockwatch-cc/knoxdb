// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// TODO
// - support expressions in fields and condition

package pack

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/util"

	. "blockwatch.cc/knoxdb/encoding/decimal"
	. "blockwatch.cc/knoxdb/vec"
)

const (
	filterThreshold = 2 // use hash map for IN conds with at least N entries
	COND_OR         = true
	COND_AND        = false
)

type Condition struct {
	Field    Field       // evaluated table field
	Mode     FilterMode  // eq|ne|gt|gte|lt|lte|in|nin|re
	Raw      string      // string value when parsed from a query string
	Value    interface{} // typed value
	From     interface{} // typed value for range queries
	To       interface{} // typed value for range queries
	IsSorted bool        // IN/NIN condition slice is already pre-sorted

	// internal data and statistics
	processed    bool                // condition has been processed already
	nomatch      bool                // condition is empty (used on index matches)
	hashmap      map[uint64]int      // compiled hashmap for byte/string set queries
	hashoverflow []hashvalue         // hash collision overflow list (one for all)
	int256map    map[Int256]struct{} // compiled int64 map for set membership
	int128map    map[Int128]struct{} // compiled int64 map for set membership
	int64map     map[int64]struct{}  // compiled int64 map for set membership
	int32map     map[int32]struct{}  // compiled int32 map for set membership
	int16map     map[int16]struct{}  // compiled int16 map for set membership
	int8map      map[int8]struct{}   // compiled int8 map for set membership
	uint64map    map[uint64]struct{} // compiled uint64 map for set membership
	uint32map    map[uint32]struct{} // compiled uint32 map for set membership
	uint16map    map[uint16]struct{} // compiled uint16 map for set membership
	uint8map     map[uint8]struct{}  // compiled uint8 map for set membership
	numValues    int                 // number of values when Value is a slice
}

// condition that is not bound to a table field yet
type UnboundCondition struct {
	Name     string
	Mode     FilterMode  // eq|ne|gt|gte|lt|lte|in|nin|re
	Raw      string      // string value when parsed from a query string
	Value    interface{} // typed value
	From     interface{} // typed value for range queries
	To       interface{} // typed value for range queries
	OrKind   bool
	Children []UnboundCondition
}

func (u UnboundCondition) Bind(table *Table) ConditionTreeNode {
	// bind single condition leaf node
	if u.Name != "" {
		return ConditionTreeNode{
			Cond: &Condition{
				Field: table.Fields().Find(u.Name),
				Mode:  u.Mode,
				Raw:   u.Raw,
				Value: u.Value,
				From:  u.From,
				To:    u.To,
			},
		}
	}

	// bind children
	node := ConditionTreeNode{
		OrKind:   u.OrKind,
		Children: make([]ConditionTreeNode, 0),
	}
	for _, v := range u.Children {
		node.Children = append(node.Children, v.Bind(table))
	}
	return node
}

func And(conds ...UnboundCondition) UnboundCondition {
	return UnboundCondition{
		Mode:     FilterModeInvalid,
		OrKind:   COND_AND,
		Children: conds,
	}
}

func Or(conds ...UnboundCondition) UnboundCondition {
	return UnboundCondition{
		Mode:     FilterModeInvalid,
		OrKind:   COND_OR,
		Children: conds,
	}
}

func Equal(field string, val interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeEqual, Value: val}
}

func NotEqual(field string, val interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeNotEqual, Value: val}
}

func In(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeIn, Value: value}
}

func NotIn(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeNotIn, Value: value}
}

func Lt(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeLt, Value: value}
}

func Lte(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeLte, Value: value}
}

func Gt(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeGt, Value: value}
}

func Gte(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeGte, Value: value}
}

func Regexp(field string, value interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeRegexp, Value: value}
}

func Range(field string, from, to interface{}) UnboundCondition {
	return UnboundCondition{Name: field, Mode: FilterModeRange, From: from, To: to}
}

type hashvalue struct {
	hash uint64
	pos  int
}

// returns the number of values to compare 1 (other), 2 (RANGE), many (IN)
func (c Condition) NValues() int {
	return c.numValues
}

func (c *Condition) EnsureTypes() error {
	// check condition values are of correct type for field
	var err error
	switch c.Mode {
	case FilterModeRange:
		// expects From and To to be set
		if c.From == nil || c.To == nil {
			return fmt.Errorf("range condition expects From and To values")
		}
		if c.From, err = c.Field.Type.CastType(c.From, c.Field); err != nil {
			return err
		}
		if c.To, err = c.Field.Type.CastType(c.To, c.Field); err != nil {
			return err
		}
		if c.Field.Type.Gt(c.From, c.To) {
			return fmt.Errorf("range condition mismatch: from > to")
		}
	case FilterModeIn, FilterModeNotIn:
		// expects a slice of values
		if c.Value, err = c.Field.Type.CastSliceType(c.Value, c.Field); err != nil {
			return err
		}
	case FilterModeRegexp:
		// expects string only
		if err := FieldTypeString.CheckType(c.Value); err != nil {
			return err
		}
	default:
		// c.Value is a simple value type
		if c.Value, err = c.Field.Type.CastType(c.Value, c.Field); err != nil {
			return err
		}
	}
	return nil
}

func (c *Condition) Compile() (err error) {
	if err = c.EnsureTypes(); err != nil {
		err = fmt.Errorf("%s cond %s: %v", c.Field.Name, c.String(), err)
		return
	}

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

	// SCALE decimal values to field scale and CONVERT slice types to underlying
	// storage for comparison, this works for parsed values and programmatic
	// conditions
	switch c.Field.Type {
	case FieldTypeDecimal32:
		if val, ok := c.Value.([]Decimal32); ok {
			// internal comparators use always the decimal's base type
			conv := make([]int32, len(val))
			for i := range val {
				conv[i] = val[i].Quantize(c.Field.Scale).Int32()
			}
			c.Value = conv
			c.numValues = len(val)
		}
		if val, ok := c.Value.(Decimal32); ok {
			c.Value = val.Quantize(c.Field.Scale)
		}
		if from, ok := c.From.(Decimal32); ok {
			c.From = from.Quantize(c.Field.Scale)
		}
		if to, ok := c.To.(Decimal32); ok {
			c.To = to.Quantize(c.Field.Scale)
		}
	case FieldTypeDecimal64:
		if val, ok := c.Value.([]Decimal64); ok {
			// internal comparators use always the decimal's base type
			conv := make([]int64, len(val))
			for i := range val {
				conv[i] = val[i].Quantize(c.Field.Scale).Int64()
			}
			c.Value = conv
			c.numValues = len(val)
		}
		if val, ok := c.Value.(Decimal64); ok {
			c.Value = val.Quantize(c.Field.Scale)
		}
		if from, ok := c.From.(Decimal64); ok {
			c.From = from.Quantize(c.Field.Scale)
		}
		if to, ok := c.To.(Decimal64); ok {
			c.To = to.Quantize(c.Field.Scale)
		}
	case FieldTypeDecimal128:
		if val, ok := c.Value.([]Decimal128); ok {
			// internal comparators use always the decimal's base type
			conv := make([]Int128, len(val))
			for i := range val {
				conv[i] = val[i].Quantize(c.Field.Scale).Int128()
			}
			c.Value = conv
			c.numValues = len(val)
		}
		if val, ok := c.Value.(Decimal128); ok {
			c.Value = val.Quantize(c.Field.Scale)
		}
		if from, ok := c.From.(Decimal128); ok {
			c.From = from.Quantize(c.Field.Scale)
		}
		if to, ok := c.To.(Decimal128); ok {
			c.To = to.Quantize(c.Field.Scale)
		}
	case FieldTypeDecimal256:
		if val, ok := c.Value.([]Decimal256); ok {
			// internal comparators use always the decimal's base type
			conv := make([]Int256, len(val))
			for i := range val {
				conv[i] = val[i].Quantize(c.Field.Scale).Int256()
			}
			c.Value = conv
			c.numValues = len(val)
		}
		if val, ok := c.Value.(Decimal256); ok {
			c.Value = val.Quantize(c.Field.Scale)
		}
		if from, ok := c.From.(Decimal256); ok {
			c.From = from.Quantize(c.Field.Scale)
		}
		if to, ok := c.To.(Decimal256); ok {
			c.To = to.Quantize(c.Field.Scale)
		}
	case FieldTypeDatetime:
		// only convert slice values used in in/nin conditions to int64 slice
		if val, ok := c.Value.([]time.Time); ok {
			conv := make([]int64, len(val))
			for i := range val {
				conv[i] = val[i].UTC().UnixNano()
			}
			c.Value = conv
			c.numValues = len(val)
		}
		return
	}

	// anything but IN, NIN is done here
	switch c.Mode {
	case FilterModeIn, FilterModeNotIn:
		// handled below
	default:
		return
	}

	// hash maps are only used for expensive types, other types
	// will use a standard go map (and hashing in Go's runtime)
	var vals [][]byte

	// sort original input slices (required for later checks in FieldType.In/InAt)
	switch c.Field.Type {
	case FieldTypeBytes:
		// require [][]byte slice as value type
		vals = c.Value.([][]byte)
		c.numValues = len(vals)
		if c.numValues == 0 {
			return
		}
		// sorted slice is always required for InBetween pack matches
		if !c.IsSorted {
			Bytes.Sort(vals) // sorts in-place
			c.IsSorted = true
		}
		// below min size a hash map is more expensive than memcmp
		if c.numValues < filterThreshold {
			return
		}
	case FieldTypeString:
		slice := c.Value.([]string)
		c.numValues = len(slice)
		if c.numValues == 0 {
			return
		}
		// sorted slice is always required for InBetween pack matches
		if !c.IsSorted {
			Strings.Sort(slice) // sorts in-place
			c.IsSorted = true
		}
		// below min size a hash map is more expensive than memcmp
		if c.numValues < filterThreshold {
			return
		}
		// convert to []byte for feeding the hash map below
		vals = make([][]byte, len(slice))
		for i, v := range slice {
			vals[i] = []byte(v)
		}
	case FieldTypeBoolean:
		slice := c.Value.([]bool)
		if slice != nil {
			hasTrue := Booleans.Contains(slice, true)
			hasFalse := Booleans.Contains(slice, false)
			if hasTrue && hasTrue == hasFalse {
				c.numValues = 2
				c.Value = []bool{false, true}
			} else {
				c.numValues = 1
				c.Value = []bool{hasTrue}
			}
		}
		return
	case FieldTypeInt256, FieldTypeDecimal256:
		slice := c.Value.([]Int256)
		if slice != nil {
			c.numValues = len(slice)
			c.int256map = make(map[Int256]struct{}, len(slice))
			for _, v := range slice {
				c.int256map[v] = struct{}{}
			}
		}
		return
	case FieldTypeInt128, FieldTypeDecimal128:
		slice := c.Value.([]Int128)
		if slice != nil {
			c.numValues = len(slice)
			c.int128map = make(map[Int128]struct{}, len(slice))
			for _, v := range slice {
				c.int128map[v] = struct{}{}
			}
		}
		return
	case FieldTypeInt64, FieldTypeDecimal64, FieldTypeDatetime:
		slice := c.Value.([]int64)
		if slice != nil {
			c.numValues = len(slice)
			c.int64map = make(map[int64]struct{}, len(slice))
			for _, v := range slice {
				c.int64map[v] = struct{}{}
			}
		}
		return
	case FieldTypeInt32, FieldTypeDecimal32:
		slice := c.Value.([]int32)
		if slice != nil {
			c.numValues = len(slice)
			c.int32map = make(map[int32]struct{}, len(slice))
			for _, v := range slice {
				c.int32map[v] = struct{}{}
			}
		}
		return
	case FieldTypeInt16:
		slice := c.Value.([]int16)
		if slice != nil {
			c.numValues = len(slice)
			c.int16map = make(map[int16]struct{}, len(slice))
			for _, v := range slice {
				c.int16map[v] = struct{}{}
			}
		}
		return
	case FieldTypeInt8:
		slice := c.Value.([]int8)
		if slice != nil {
			c.numValues = len(slice)
			c.int8map = make(map[int8]struct{}, len(slice))
			for _, v := range slice {
				c.int8map[v] = struct{}{}
			}
		}
		return
	case FieldTypeUint64:
		slice := c.Value.([]uint64)
		c.numValues = len(slice)
		if c.numValues == 0 {
			return
		}
		// use a map for lookups unless we check sorted pk slices
		if c.Field.Flags&FlagPrimary > 0 {
			if !c.IsSorted {
				c.Value = Uint64.Sort(slice)
				c.IsSorted = true
			}
		} else {
			c.uint64map = make(map[uint64]struct{}, len(slice))
			for _, v := range slice {
				c.uint64map[v] = struct{}{}
			}
		}
		return
	case FieldTypeUint32:
		slice := c.Value.([]uint32)
		if slice != nil {
			c.numValues = len(slice)
			c.uint32map = make(map[uint32]struct{}, len(slice))
			for _, v := range slice {
				c.uint32map[v] = struct{}{}
			}
		}
		return
	case FieldTypeUint16:
		slice := c.Value.([]uint16)
		if slice != nil {
			c.numValues = len(slice)
			c.uint16map = make(map[uint16]struct{}, len(slice))
			for _, v := range slice {
				c.uint16map[v] = struct{}{}
			}
		}
		return
	case FieldTypeUint8:
		slice := c.Value.([]uint8)
		if slice != nil {
			c.numValues = len(slice)
			c.uint8map = make(map[uint8]struct{}, len(slice))
			for _, v := range slice {
				c.uint8map[v] = struct{}{}
			}
		}
		return
	case FieldTypeFloat64:
		slice := c.Value.([]float64)
		if slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				c.Value = Float64.Sort(slice)
				c.IsSorted = true
			}
		}
		return
	case FieldTypeFloat32:
		slice := c.Value.([]float32)
		if slice != nil {
			c.numValues = len(slice)
			if !c.IsSorted {
				c.Value = Float32.Sort(slice)
				c.IsSorted = true
			}
		}
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
	return nil
}

// match package min/max values against the condition
// Note: min/max are raw storage values (i.e. for decimals, they are scaled integers)
func (c Condition) MaybeMatchPack(info PackInfo) bool {
	min, max := info.Blocks[c.Field.Index].MinValue, info.Blocks[c.Field.Index].MaxValue
	scale := c.Field.Scale
	// decimals only: convert storage type used in block info to field type
	switch c.Field.Type {
	case FieldTypeDecimal32:
		min = NewDecimal32(min.(int32), scale)
		max = NewDecimal32(max.(int32), scale)
	case FieldTypeDecimal64:
		min = NewDecimal64(min.(int64), scale)
		max = NewDecimal64(max.(int64), scale)
	case FieldTypeDecimal128:
		min = NewDecimal128(min.(Int128), scale)
		max = NewDecimal128(max.(Int128), scale)
	case FieldTypeDecimal256:
		min = NewDecimal256(min.(Int256), scale)
		max = NewDecimal256(max.(Int256), scale)
	}
	// compare pack header
	switch c.Mode {
	case FilterModeEqual:
		// condition value is within range
		return c.Field.Type.Between(c.Value, min, max)
	case FilterModeNotEqual:
		return true // we don't know, so full scan is required
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
		// min OR max is > condition value
		return c.Field.Type.Gt(min, c.Value) || c.Field.Type.Gt(max, c.Value)
	case FilterModeGte:
		// min OR max is >= condition value
		return c.Field.Type.Gte(min, c.Value) || c.Field.Type.Gte(max, c.Value)
	case FilterModeLt:
		// min OR max is < condition value
		return c.Field.Type.Lt(min, c.Value) || c.Field.Type.Lt(max, c.Value)
	case FilterModeLte:
		// min OR max is <= condition value
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
			return fmt.Sprintf("%s %s %v", c.Field.Name, c.Mode.Op(), c.Field.Type.SliceToString(c.Value, c.Field))
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
		c.From, err = c.Field.Type.ParseAs(vv[0], c.Field)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
		c.To, err = c.Field.Type.ParseAs(vv[1], c.Field)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	case FilterModeIn, FilterModeNotIn:
		c.Value, err = c.Field.Type.ParseSliceAs(val, c.Field)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	default:
		c.Value, err = c.Field.Type.ParseAs(val, c.Field)
		if err != nil {
			return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
		}
	}
	return c, nil
}

// MatchPack matches all elements in package pkg against the defined condition
// and returns a bitset of the same length as the package with bits set to true
// where the match is successful.
//
// This implementation uses low level block vectors to efficiently execute
// vectorized checks with custom assembly-optimized routines.
func (c Condition) MatchPack(pkg *Package, mask *Bitset) *Bitset {
	bits := NewBitset(pkg.Len())
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
			for i, v := range block.Int256 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int256map[v]; ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt128, FieldTypeDecimal128:
			for i, v := range block.Int128 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int128map[v]; ok {
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
			if pkg.key == journalKey && c.uint64map == nil {
				c.uint64map = make(map[uint64]struct{}, len(in))
				for _, v := range in {
					c.uint64map[v] = struct{}{}
				}
			}
			if pkg.key != journalKey && c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
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
				for i, v := range block.Bytes {
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
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
				}
			} else {
				for i, v := range block.Bytes {
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
				for i, v := range block.Strings {
					// skip masked values
					if mask != nil && !mask.IsSet(i) {
						continue
					}
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
				}
			} else {
				for i, v := range block.Strings {
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
		}

		return bits

	case FilterModeNotIn:
		// unlike with the other types we use the compiled maps and run
		// the matching loop here rather than using vectorized functions
		//
		// type check was already performed in compile stage
		switch c.Field.Type {
		case FieldTypeInt256, FieldTypeDecimal256:
			for i, v := range block.Int256 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int256map[v]; !ok {
					bits.Set(i)
				}
			}
		case FieldTypeInt128, FieldTypeDecimal128:
			for i, v := range block.Int128 {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
				if _, ok := c.int128map[v]; !ok {
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
			if pkg.key == journalKey && c.uint64map == nil {
				c.uint64map = make(map[uint64]struct{}, len(in))
				for _, v := range in {
					c.uint64map[v] = struct{}{}
				}
			}
			if pkg.key != journalKey && c.Field.Flags&FlagPrimary > 0 && len(in) > 0 {
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
			for i, v := range block.Bytes {
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
			for i, v := range block.Strings {
				// skip masked values
				if mask != nil && !mask.IsSet(i) {
					continue
				}
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
					if !c.Field.Type.In(v, c.Value) {
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

type ConditionTreeNode struct {
	OrKind   bool                // AND|OR
	Children []ConditionTreeNode // sub conditions
	Cond     *Condition          // ptr to condition
}

func (n ConditionTreeNode) Empty() bool {
	return len(n.Children) == 0 && n.Cond == nil
}

func (n ConditionTreeNode) Leaf() bool {
	return n.Cond != nil
}

func (n ConditionTreeNode) NoMatch() bool {
	if n.Empty() {
		return false
	}

	if n.Leaf() {
		return n.Cond.nomatch
	}

	if n.OrKind {
		for _, v := range n.Children {
			if !v.NoMatch() {
				return false
			}
		}
		return true
	} else {
		for _, v := range n.Children {
			if v.NoMatch() {
				return true
			}
		}
		return false
	}
}

// may otimize (reduce/merge/replace) conditions in the future
func (n ConditionTreeNode) Compile() error {
	if n.Leaf() {
		if err := n.Cond.Compile(); err != nil {
			return nil
		}
	} else {
		for _, v := range n.Children {
			if err := v.Compile(); err != nil {
				return err
			}
		}
	}
	return nil
}

// returns unique list of fields
func (n ConditionTreeNode) Fields() FieldList {
	if n.Empty() {
		return nil
	}
	if n.Leaf() {
		return FieldList{n.Cond.Field}
	}
	fl := make(FieldList, 0)
	for _, v := range n.Children {
		fl.AddUnique(v.Fields()...)
	}
	return fl
}

// returns the decision tree size (including sub-conditions)
func (n ConditionTreeNode) Weight() int {
	if n.Leaf() {
		return n.Cond.NValues()
	}
	w := 0
	for _, v := range n.Children {
		w += v.Weight()
	}
	return w
}

// returns the subtree execution cost based on the number of rows
// that may be visited in the given pack for a full scan times the
// number of comparisons
func (n ConditionTreeNode) Cost(info PackInfo) int {
	return n.Weight() * info.NValues
}

func (n ConditionTreeNode) Conditions() []*Condition {
	if n.Leaf() {
		return []*Condition{n.Cond}
	}
	cond := make([]*Condition, 0)
	for _, v := range n.Children {
		cond = append(cond, v.Conditions()...)
	}
	return cond
}

func (n *ConditionTreeNode) AddAndCondition(c *Condition) {
	// create a new root when operation changes
	if n.OrKind || n.Leaf() {
		clone := ConditionTreeNode{
			OrKind:   n.OrKind,
			Children: n.Children,
			Cond:     n.Cond,
		}
		n.OrKind = COND_AND
		n.Children = []ConditionTreeNode{clone}
	}

	// append new condition to this element
	n.Children = append(n.Children, ConditionTreeNode{Cond: c})
}

func (n *ConditionTreeNode) AddOrCondition(c *Condition) {
	// create a new root when operation changes
	if !n.OrKind || n.Leaf() {
		clone := ConditionTreeNode{
			OrKind:   n.OrKind,
			Children: n.Children,
			Cond:     n.Cond,
		}
		n.OrKind = COND_OR
		n.Children = []ConditionTreeNode{clone}
	}

	// append new condition to this element
	n.Children = append(n.Children, ConditionTreeNode{Cond: c})
}

func (n *ConditionTreeNode) ReplaceNode(node ConditionTreeNode) {
	n.Cond = node.Cond
	n.OrKind = node.OrKind
	n.Children = node.Children
}

func (n *ConditionTreeNode) AddNode(node ConditionTreeNode) {
	// create a new root when operation changes
	if n.Leaf() || (!node.Leaf() && n.OrKind != node.OrKind) {
		clone := ConditionTreeNode{
			OrKind:   n.OrKind,
			Children: n.Children,
			Cond:     n.Cond,
		}
		n.OrKind = node.OrKind
		n.Children = []ConditionTreeNode{clone}
	}

	// append new condition to this element
	if node.Leaf() {
		n.Children = append(n.Children, node)
	} else {
		n.Children = append(n.Children, node.Children...)
	}
}

func (n ConditionTreeNode) MaybeMatchPack(info PackInfo) bool {
	// never visit empty packs
	if info.NValues == 0 {
		return false
	}
	// always match empty condition nodes
	if n.Empty() {
		return true
	}
	// match single leafs
	if n.Leaf() {
		return n.Cond.MaybeMatchPack(info)
	}
	// combine leaf decisions along the tree
	for _, v := range n.Children {
		if n.OrKind {
			// for OR nodes, stop at the first successful hint
			if v.MaybeMatchPack(info) {
				return true
			}
		} else {
			// for AND nodes stop at the first non-successful hint
			if !v.MaybeMatchPack(info) {
				return false
			}
		}
	}

	// when all AND nodes match
	return true
}

func (n ConditionTreeNode) MatchPack(pkg *Package, info PackInfo) *Bitset {
	// if root contains a snigle leaf only, match it
	if n.Leaf() {
		return n.Cond.MatchPack(pkg, nil)
	}

	// if root is empty and no leaf is defined, return a full match
	if n.Empty() {
		return NewBitset(pkg.Len()).One()
	}

	// process all children
	if n.OrKind {
		return n.MatchPackOr(pkg, info)
	} else {
		return n.MatchPackAnd(pkg, info)
	}
}

// Return a bit vector containing matching positions in the pack combining
// multiple AND conditions with efficient skipping and aggregation.
// TODO: consider concurrent matches for multiple conditions and cascading bitset merge
func (n ConditionTreeNode) MatchPackAnd(pkg *Package, info PackInfo) *Bitset {
	// start with a full bitset
	bits := NewBitset(pkg.Len()).One()

	// match conditions and merge bit vectors
	// stop early when result contains all zeros (assuming AND relation)
	// always match empty condition list
	for _, cn := range n.Children {
		var b *Bitset
		if !cn.Leaf() {
			// recurse into another AND or OR condition subtree
			b = cn.MatchPack(pkg, info)
		} else {
			c := cn.Cond
			// Quick inclusion check to skip matching when the current condition
			// would return an all-true vector. Note that we do not have to check
			// for an all-false vector because MaybeMatchPack() has already deselected
			// packs of that kind (except the journal)
			//
			// We exclude journal from quick check because we cannot rely on
			// min/max values.
			//
			if pkg.key != journalKey && len(info.Blocks) > c.Field.Index {
				blockInfo := info.Blocks[c.Field.Index]
				min, max := blockInfo.MinValue, blockInfo.MaxValue
				switch c.Mode {
				case FilterModeEqual:
					// condition is always true iff min == max == c.Value
					if c.Field.Type.Equal(min, c.Value) && c.Field.Type.Equal(max, c.Value) {
						continue
					}
				case FilterModeNotEqual:
					// condition is always true iff c.Value < min || c.Value > max
					if c.Field.Type.Lt(c.Value, min) || c.Field.Type.Gt(c.Value, max) {
						continue
					}
				case FilterModeRange:
					// condition is always true iff pack range <= condition range
					if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
						continue
					}
				case FilterModeGt:
					// condition is always true iff min > c.Value
					if c.Field.Type.Gt(min, c.Value) {
						continue
					}
				case FilterModeGte:
					// condition is always true iff min >= c.Value
					if c.Field.Type.Gte(min, c.Value) {
						continue
					}
				case FilterModeLt:
					// condition is always true iff max < c.Value
					if c.Field.Type.Lt(max, c.Value) {
						continue
					}
				case FilterModeLte:
					// condition is always true iff max <= c.Value
					if c.Field.Type.Lte(max, c.Value) {
						continue
					}
				}
			}

			// match vector against condition using last match as mask
			b = c.MatchPack(pkg, bits)
		}

		// shortcut
		if bits.Count() == bits.Len() {
			bits.Close()
			bits = b
			continue
		}

		// merge
		_, any := bits.And(b)
		b.Close()

		// early stop on empty aggregate match
		if any == 0 {
			break
		}
	}
	return bits
}

// Return a bit vector containing matching positions in the pack combining
// multiple OR conditions with efficient skipping and aggregation.
func (n ConditionTreeNode) MatchPackOr(pkg *Package, info PackInfo) *Bitset {
	// start with an empty bitset
	bits := NewBitset(pkg.Len())

	// match conditions and merge bit vectors
	// stop early when result contains all ones (assuming OR relation)
	for _, cn := range n.Children {
		var b *Bitset
		if !cn.Leaf() {
			// recurse into another AND or OR condition subtree
			b = cn.MatchPack(pkg, info)
		} else {
			c := cn.Cond
			// Quick inclusion check to skip matching when the current condition
			// would return an all-true vector. Note that we do not have to check
			// for an all-false vector because MaybeMatchPack() has already deselected
			// packs of that kind (except the journal).
			//
			// We exclude journal from quick check because we cannot rely on
			// min/max values.
			//
			if pkg.key != journalKey && len(info.Blocks) > c.Field.Index {
				blockInfo := info.Blocks[c.Field.Index]
				min, max := blockInfo.MinValue, blockInfo.MaxValue
				skipEarly := false
				switch c.Mode {
				case FilterModeEqual:
					// condition is always true iff min == max == c.Value
					if c.Field.Type.Equal(min, c.Value) && c.Field.Type.Equal(max, c.Value) {
						skipEarly = true
					}
				case FilterModeNotEqual:
					// condition is always true iff c.Value < min || c.Value > max
					if c.Field.Type.Lt(c.Value, min) || c.Field.Type.Gt(c.Value, max) {
						skipEarly = true
					}
				case FilterModeRange:
					// condition is always true iff pack range <= condition range
					if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
						skipEarly = true
					}
				case FilterModeGt:
					// condition is always true iff min > c.Value
					if c.Field.Type.Gt(min, c.Value) {
						skipEarly = true
					}
				case FilterModeGte:
					// condition is always true iff min >= c.Value
					if c.Field.Type.Gte(min, c.Value) {
						skipEarly = true
					}
				case FilterModeLt:
					// condition is always true iff max < c.Value
					if c.Field.Type.Lt(max, c.Value) {
						skipEarly = true
					}
				case FilterModeLte:
					// condition is always true iff max <= c.Value
					if c.Field.Type.Lte(max, c.Value) {
						skipEarly = true
					}
				}
				if skipEarly {
					bits.Close()
					return NewBitset(pkg.Len()).One()
				}
			}

			// match vector against condition using last match as mask
			b = c.MatchPack(pkg, bits)
		}

		// shortcut
		if b.Count() == 0 {
			b.Close()
			continue
		}

		// merge
		bits.Or(b)
		b.Close()

		// early stop on full aggregate match
		if bits.Count() == bits.Len() {
			break
		}
	}
	return bits
}

func (n ConditionTreeNode) MatchAt(pkg *Package, pos int) bool {
	// if root contains a snigle leaf only, match it
	if n.Leaf() {
		return n.Cond.MatchAt(pkg, pos)
	}

	// if root is empty and no leaf is defined, return a full match
	if n.Empty() {
		return true
	}

	// process all children
	if n.OrKind {
		for _, c := range n.Children {
			if c.MatchAt(pkg, pos) {
				return true
			}
		}
	} else {
		for _, c := range n.Children {
			if !c.MatchAt(pkg, pos) {
				return false
			}
		}
	}
	return true
}
