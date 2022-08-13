// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
    "fmt"
    "time"

    "blockwatch.cc/knoxdb/filter/bloom"
    "blockwatch.cc/knoxdb/hash/xxhash"

    "blockwatch.cc/knoxdb/encoding/compress"
    "blockwatch.cc/knoxdb/encoding/decimal"
    "blockwatch.cc/knoxdb/vec"
)

const (
    filterThreshold = 2 // use hash map for IN conds with at least N entries
)

func (c *Condition) Compile() (err error) {
    if !c.Field.IsValid() {
        err = fmt.Errorf("invalid field in cond %s", c.String())
        return
    }

    // cast user defined types into Go types supported by internal matching
    // algorithms, e.g. convert BinaryMarshaler interface type to []byte,
    // convert enum types to native int8/16/32/64
    if err = c.ensureTypes(); err != nil {
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

    // map decimal and date types to internal representations
    // count number of values for slice types
    c.convertInputTypes()

    switch c.Mode {
    case FilterModeIn, FilterModeNotIn:
        c.sortValueSlice()
        c.buildValueMap()
        c.buildHashMap()
        c.buildBloomData()
    default:
        if c.Field.Flags.Contains(FlagBloom) {
            c.bloomHashes = [][2]uint32{c.Field.Type.Hash(c.Value)}
        }
    }
    return nil
}

func (c *Condition) ensureTypes() error {
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

// SCALE decimal values to field scale and CONVERT slice types to underlying
// storage for comparison, this works for parsed values and programmatic
// conditions
func (c *Condition) convertInputTypes() {
    switch c.Field.Type {
    case FieldTypeDecimal32:
        if val, ok := c.Value.([]decimal.Decimal32); ok {
            // internal comparators use always the decimal's base type
            conv := make([]int32, len(val))
            for i := range val {
                conv[i] = val[i].Quantize(c.Field.Scale).Int32()
            }
            c.Value = conv
            c.numValues = len(val)
        }
        if val, ok := c.Value.(decimal.Decimal32); ok {
            c.Value = val.Quantize(c.Field.Scale)
        }
        if from, ok := c.From.(decimal.Decimal32); ok {
            c.From = from.Quantize(c.Field.Scale)
        }
        if to, ok := c.To.(decimal.Decimal32); ok {
            c.To = to.Quantize(c.Field.Scale)
        }
    case FieldTypeDecimal64:
        if val, ok := c.Value.([]decimal.Decimal64); ok {
            // internal comparators use always the decimal's base type
            conv := make([]int64, len(val))
            for i := range val {
                conv[i] = val[i].Quantize(c.Field.Scale).Int64()
            }
            c.Value = conv
            c.numValues = len(val)
        }
        if val, ok := c.Value.(decimal.Decimal64); ok {
            c.Value = val.Quantize(c.Field.Scale)
        }
        if from, ok := c.From.(decimal.Decimal64); ok {
            c.From = from.Quantize(c.Field.Scale)
        }
        if to, ok := c.To.(decimal.Decimal64); ok {
            c.To = to.Quantize(c.Field.Scale)
        }
    case FieldTypeDecimal128:
        if val, ok := c.Value.([]decimal.Decimal128); ok {
            // internal comparators use always the decimal's base type
            conv := make([]vec.Int128, len(val))
            for i := range val {
                conv[i] = val[i].Quantize(c.Field.Scale).Int128()
            }
            c.Value = conv
            c.numValues = len(val)
        }
        if val, ok := c.Value.(decimal.Decimal128); ok {
            c.Value = val.Quantize(c.Field.Scale)
        }
        if from, ok := c.From.(decimal.Decimal128); ok {
            c.From = from.Quantize(c.Field.Scale)
        }
        if to, ok := c.To.(decimal.Decimal128); ok {
            c.To = to.Quantize(c.Field.Scale)
        }
    case FieldTypeDecimal256:
        if val, ok := c.Value.([]decimal.Decimal256); ok {
            // internal comparators use always the decimal's base type
            conv := make([]vec.Int256, len(val))
            for i := range val {
                conv[i] = val[i].Quantize(c.Field.Scale).Int256()
            }
            c.Value = conv
            c.numValues = len(val)
        }
        if val, ok := c.Value.(decimal.Decimal256); ok {
            c.Value = val.Quantize(c.Field.Scale)
        }
        if from, ok := c.From.(decimal.Decimal256); ok {
            c.From = from.Quantize(c.Field.Scale)
        }
        if to, ok := c.To.(decimal.Decimal256); ok {
            c.To = to.Quantize(c.Field.Scale)
        }
    case FieldTypeDatetime:
        // only convert slice values used in in/nin conditions to int64 slice
        if slice, ok := c.Value.([]time.Time); ok {
            conv := make([]int64, len(slice))
            for i := range slice {
                conv[i] = slice[i].UTC().UnixNano()
            }
            c.Value = conv
            c.numValues = len(slice)
        }
    case FieldTypeInt256:
        if slice, ok := c.Value.([]vec.Int256); ok {
            c.numValues = len(slice)
        }
    case FieldTypeInt128:
        if slice, ok := c.Value.([]vec.Int128); ok {
            c.numValues = len(slice)
        }
    case FieldTypeInt64:
        if slice, ok := c.Value.([]int64); ok {
            c.numValues = len(slice)
        }
    case FieldTypeInt32:
        if slice, ok := c.Value.([]int32); ok {
            c.numValues = len(slice)
        }
    case FieldTypeInt16:
        if slice, ok := c.Value.([]int16); ok {
            c.numValues = len(slice)
        }
    case FieldTypeInt8:
        if slice, ok := c.Value.([]int8); ok {
            c.numValues = len(slice)
        }
    case FieldTypeBytes:
        if slice, ok := c.Value.([][]byte); ok {
            c.numValues = len(slice)
        }
    case FieldTypeString:
        if slice, ok := c.Value.([]string); ok {
            c.numValues = len(slice)
        }
    case FieldTypeUint64:
        if slice, ok := c.Value.([]uint64); ok {
            c.numValues = len(slice)
        }
    case FieldTypeUint32:
        if slice, ok := c.Value.([]uint32); ok {
            c.numValues = len(slice)
        }
    case FieldTypeUint16:
        if slice, ok := c.Value.([]uint16); ok {
            c.numValues = len(slice)
        }
    case FieldTypeUint8:
        if slice, ok := c.Value.([]uint8); ok {
            c.numValues = len(slice)
        }
    case FieldTypeFloat64:
        if slice, ok := c.Value.([]float64); ok {
            c.numValues = len(slice)
        }
    case FieldTypeFloat32:
        if slice, ok := c.Value.([]float32); ok {
            c.numValues = len(slice)
        }
    case FieldTypeBoolean:
        if slice, ok := c.Value.([]bool); ok {
            hasTrue := vec.Booleans.Contains(slice, true)
            hasFalse := vec.Booleans.Contains(slice, false)
            if hasTrue && hasTrue == hasFalse {
                c.Value = []bool{false, true}
                c.numValues = 2
            } else {
                c.Value = []bool{hasTrue}
                c.numValues = 1
            }
        }
    }
}

// Sorted slices are required for checks in FieldType.In/InAt/InBetween
func (c *Condition) sortValueSlice() {
    if c.IsSorted {
        return
    }
    switch c.Field.Type {
    case FieldTypeBytes:
        if slice := c.Value.([][]byte); slice != nil {
            vec.Bytes.Sort(slice) // sorts in-place
        }
    case FieldTypeString:
        if slice := c.Value.([]string); slice != nil {
            vec.Strings.Sort(slice) // sorts in-place
        }
    case FieldTypeUint64:
        if slice := c.Value.([]uint64); slice != nil {
            c.Value = vec.Uint64.Sort(slice)
        }
    case FieldTypeUint32:
        if slice := c.Value.([]uint32); slice != nil {
            c.Value = vec.Uint32.Sort(slice)
        }
    case FieldTypeUint16:
        if slice := c.Value.([]uint16); slice != nil {
            c.Value = vec.Uint16.Sort(slice)
        }
    case FieldTypeUint8:
        if slice := c.Value.([]uint8); slice != nil {
            c.Value = vec.Uint8.Sort(slice)
        }
    case FieldTypeInt256, FieldTypeDecimal256:
        if slice := c.Value.([]vec.Int256); slice != nil {
            vec.Int256Sorter(slice).Sort()
        }
    case FieldTypeInt128, FieldTypeDecimal128:
        if slice := c.Value.([]vec.Int128); slice != nil {
            vec.Int128Sorter(slice).Sort()
        }
    case FieldTypeInt64, FieldTypeDecimal64, FieldTypeDatetime:
        if slice := c.Value.([]int64); slice != nil {
            c.Value = vec.Int64.Sort(slice)
        }
    case FieldTypeInt32, FieldTypeDecimal32:
        if slice := c.Value.([]int32); slice != nil {
            c.Value = vec.Int32.Sort(slice)
        }
    case FieldTypeInt16:
        if slice := c.Value.([]int16); slice != nil {
            c.Value = vec.Int16.Sort(slice)
        }
    case FieldTypeInt8:
        if slice := c.Value.([]int8); slice != nil {
            c.Value = vec.Int8.Sort(slice)
        }
    case FieldTypeFloat64:
        if slice := c.Value.([]float64); slice != nil {
            c.Value = vec.Float64.Sort(slice)
        }
    case FieldTypeFloat32:
        if slice := c.Value.([]float32); slice != nil {
            c.Value = vec.Float32.Sort(slice)
        }
    }
    c.IsSorted = true
}

func (c *Condition) buildValueMap() {
    switch c.Field.Type {
    case FieldTypeUint64:
        if c.Field.Flags&FlagPrimary == 0 {
            if slice := c.Value.([]uint64); slice != nil {
                c.uint64map = make(map[uint64]struct{}, len(slice))
                for _, v := range slice {
                    c.uint64map[v] = struct{}{}
                }
            }
        }
    case FieldTypeUint32:
        if slice := c.Value.([]uint32); slice != nil {
            c.uint32map = make(map[uint32]struct{}, len(slice))
            for _, v := range slice {
                c.uint32map[v] = struct{}{}
            }
        }
    case FieldTypeUint16:
        if slice := c.Value.([]uint16); slice != nil {
            c.uint16map = make(map[uint16]struct{}, len(slice))
            for _, v := range slice {
                c.uint16map[v] = struct{}{}
            }
        }
    case FieldTypeUint8:
        if slice := c.Value.([]uint8); slice != nil {
            c.uint8map = make(map[uint8]struct{}, len(slice))
            for _, v := range slice {
                c.uint8map[v] = struct{}{}
            }
        }
    case FieldTypeInt256, FieldTypeDecimal256:
        if slice := c.Value.([]vec.Int256); slice != nil {
            c.int256map = make(map[vec.Int256]struct{}, len(slice))
            for _, v := range slice {
                c.int256map[v] = struct{}{}
            }
        }
    case FieldTypeInt128, FieldTypeDecimal128:
        if slice := c.Value.([]vec.Int128); slice != nil {
            c.int128map = make(map[vec.Int128]struct{}, len(slice))
            for _, v := range slice {
                c.int128map[v] = struct{}{}
            }
        }
    case FieldTypeInt64, FieldTypeDecimal64, FieldTypeDatetime:
        if slice := c.Value.([]int64); slice != nil {
            c.int64map = make(map[int64]struct{}, len(slice))
            for _, v := range slice {
                c.int64map[v] = struct{}{}
            }
        }
    case FieldTypeInt32, FieldTypeDecimal32:
        if slice := c.Value.([]int32); slice != nil {
            c.int32map = make(map[int32]struct{}, len(slice))
            for _, v := range slice {
                c.int32map[v] = struct{}{}
            }
        }
    case FieldTypeInt16:
        if slice := c.Value.([]int16); slice != nil {
            c.int16map = make(map[int16]struct{}, len(slice))
            for _, v := range slice {
                c.int16map[v] = struct{}{}
            }
        }
    case FieldTypeInt8:
        if slice := c.Value.([]int8); slice != nil {
            c.int8map = make(map[int8]struct{}, len(slice))
            for _, v := range slice {
                c.int8map[v] = struct{}{}
            }
        }
    }
}

type hashvalue struct {
    hash uint64
    pos  int
}

// Hashmaps are only used for byte and string slices in combination with IN/NIN
// conditions. Other types will use a standard go map (and hashing in Go's runtime)
func (c *Condition) buildHashMap() {
    var vals [][]byte
    switch c.Field.Type {
    case FieldTypeBytes:
        vals = c.Value.([][]byte)
        if vals == nil {
            return
        }
    case FieldTypeString:
        slice := c.Value.([]string)
        if slice == nil {
            return
        }
        // convert to []byte for feeding the hash map below
        vals = make([][]byte, len(slice))
        for i, v := range slice {
            vals[i] = compress.UnsafeGetBytes(v)
        }
    default:
        return
    }

    if len(vals) < filterThreshold {
        return
    }

    c.hashmap = make(map[uint64]int)
    for i, v := range vals {
        sum := xxhash.Sum64(v)
        if mapval, ok := c.hashmap[sum]; !ok {
            c.hashmap[sum] = i
        } else {
            if mapval != 0xFFFFFFFF {
                log.Warnf("pack: condition hash collision %0x / %0x == %0x", v, vals[mapval], sum)
                c.hashoverflow = append(c.hashoverflow, hashvalue{
                    hash: sum,
                    pos:  mapval,
                })
            } else {
                log.Warnf("pack: condition double hash collision %0x == %0x", v, sum)
            }
            c.hashoverflow = append(c.hashoverflow, hashvalue{
                hash: sum,
                pos:  i,
            })
            c.hashmap[sum] = 0xFFFFFFFF
        }
    }
    // log.Debugf("query: compiled hash map condition with size %s for %d values",
    //  util.ByteSize(len(vals)*12), len(vals))
}

func (c *Condition) buildBloomData() {
    if !c.Field.Flags.Contains(FlagBloom) {
        return
    }
    c.bloomHashes = make([][2]uint32, 0)
    switch c.Field.Type {
    case FieldTypeBytes:
        for _, val := range c.Value.([][]byte) {
            c.bloomHashes = append(c.bloomHashes, bloomVec.Hash(val))
        }
    case FieldTypeString:
        for _, val := range c.Value.([]string) {
            c.bloomHashes = append(c.bloomHashes, bloomVec.Hash(compress.UnsafeGetBytes(val)))
        }
    case FieldTypeInt256, FieldTypeDecimal256:
        for _, val := range c.Value.([]vec.Int256) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeInt128, FieldTypeDecimal128:
        for _, val := range c.Value.([]vec.Int128) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeInt64, FieldTypeDecimal64, FieldTypeDatetime:
        for _, val := range c.Value.([]int64) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeInt32, FieldTypeDecimal32:
        for _, val := range c.Value.([]int32) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeInt16:
        for _, val := range c.Value.([]int16) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeInt8:
        for _, val := range c.Value.([]int8) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeUint64:
        for _, val := range c.Value.([]uint64) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeUint32:
        for _, val := range c.Value.([]uint32) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeUint16:
        for _, val := range c.Value.([]uint16) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeUint8:
        for _, val := range c.Value.([]uint8) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeFloat64:
        for _, val := range c.Value.([]float64) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeFloat32:
        for _, val := range c.Value.([]float32) {
            c.bloomHashes = append(c.bloomHashes, c.Field.Type.Hash(val))
        }
    case FieldTypeBoolean:
        for _, val := range c.Value.([]bool) {
            var b byte
            if val {
                b = 1
            }
            c.bloomHashes = append(c.bloomHashes, bloomVec.Hash([]byte{b}))
        }
    }
}
