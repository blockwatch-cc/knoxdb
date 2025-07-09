// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
	"fmt"
	"reflect"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

type Converter interface {
	ConvertPack(pkg *pack.Package, mode pack.WriteMode) *pack.Package
	QueryKeys(node *filter.Node) []uint64
	QueryNode(node *filter.Node) *filter.Node
}

type RelinkConverter struct {
	schema    *schema.Schema
	srcBlocks []int // ordered list of src blocks to link
}

func (*RelinkConverter) QueryKeys(_ *filter.Node) []uint64 {
	// unused (hash index only)
	return nil
}

func (c *RelinkConverter) QueryNode(node *filter.Node) *filter.Node {
	// rewrite filter node to match the index pack structure
	// Note: index storage is u64
	f0 := c.schema.Fields()[0]
	flt := node.Filter
	if f0.Type() == types.FieldTypeUint64 {
		return &filter.Node{
			Filter: &filter.Filter{
				Name:    "int",
				Type:    flt.Type,
				Mode:    flt.Mode,
				Index:   0,
				Value:   flt.Value,
				Matcher: flt.Matcher,
			},
		}
	} else {
		val, err := schema.NewCaster(types.FieldTypeUint64, 0, nil).CastValue(flt.Value)
		if err != nil {
			panic(fmt.Errorf("cast index query value %T to u64: %v", flt.Value, err))
		}
		matcher := filter.NewFactory(f0.Type()).New(flt.Mode)
		matcher.WithValue(val)
		return filter.NewNode().AddLeaf(&filter.Filter{
			Name:    "int",
			Type:    flt.Type,
			Mode:    flt.Mode,
			Index:   0,
			Value:   val,
			Matcher: matcher,
		})
	}
}

func (c *RelinkConverter) ConvertPack(pkg *pack.Package, mode pack.WriteMode) *pack.Package {
	ipkg := pack.New().WithSchema(c.schema).WithMaxRows(pkg.Cap())
	for i, v := range c.srcBlocks {
		b := pkg.Block(v)

		// convert first block to u64
		if i == 0 && b.Type() != block.BlockUint64 {
			u64 := block.New(block.BlockUint64, pkg.Len())
			acc := u64.Uint64()
			switch b.Type() {
			case block.BlockInt64:
				copy(u64.Int64().Slice(), b.Int64().Slice())
			case block.BlockInt32:
				for _, v := range b.Int32().Slice() {
					acc.Append(uint64(v))
				}
			case block.BlockInt16:
				for _, v := range b.Int16().Slice() {
					acc.Append(uint64(v))
				}
			case block.BlockInt8:
				for _, v := range b.Int8().Slice() {
					acc.Append(uint64(v))
				}
			case block.BlockUint32:
				for _, v := range b.Uint32().Slice() {
					acc.Append(uint64(v))
				}
			case block.BlockUint16:
				for _, v := range b.Uint16().Slice() {
					acc.Append(uint64(v))
				}
			case block.BlockUint8:
				for _, v := range b.Uint8().Slice() {
					acc.Append(uint64(v))
				}
			}
			b = u64
		} else {
			b.Ref()
		}
		ipkg.WithBlock(i, b)
	}
	ipkg.UpdateLen()

	return ipkg
}

type SimpleHashConverter struct {
	schema    *schema.Schema
	srcBlocks []int // ordered list of table blocks to link
	hashBlock int   // table source block used for hashing
}

func (c *SimpleHashConverter) ConvertPack(pkg *pack.Package, mode pack.WriteMode) *pack.Package {
	ipkg := pack.New().WithSchema(c.schema).WithMaxRows(pkg.Cap())
	ipkg.WithBlock(0, pkg.Block(c.hashBlock).Hash())
	for i, v := range c.srcBlocks {
		b := pkg.Block(v)
		b.Ref()
		ipkg.WithBlock(i+1, b)
	}
	ipkg.UpdateLen()
	return ipkg
}

func (c *SimpleHashConverter) QueryKeys(node *filter.Node) []uint64 {
	// produce output hash (uint64) from query filter value encoded to LE wire format
	// use schema field encoding helper to translate Go types from query
	f0 := c.schema.Fields()[0]
	buf := bytes.NewBuffer(nil)
	flt := node.Filter

	switch flt.Mode {
	case types.FilterModeEqual:
		// single
		_ = f0.Encode(buf, flt.Value, LE)
		return []uint64{xxhash64.Sum64(buf.Bytes())}

	case types.FilterModeIn, types.FilterModeNotIn:
		// slice
		rval := reflect.ValueOf(flt.Value)
		if rval.Kind() != reflect.Slice {
			return nil
		}
		res := make([]uint64, rval.Len())
		for i := range res {
			buf.Reset()
			_ = f0.Encode(buf, rval.Index(i).Interface(), LE)
			res[i] = xxhash64.Sum64(buf.Bytes())
		}
		return res

	default:
		// unreachable
		assert.Unreachable("invalid filter mode for pack hash query", "mode", flt.Mode)
		return nil
	}
}

func (*SimpleHashConverter) QueryNode(_ *filter.Node) *filter.Node {
	// unused (range index scans only)
	return nil
}

type CompositeHashConverter struct {
	idxSchema  *schema.Schema
	srcSchema  *schema.Schema
	srcBlocks  []int // ordered list of src blocks to link
	hashBlocks []int // ordered list of blocks to hash
}

func (c *CompositeHashConverter) ConvertPack(pkg *pack.Package, mode pack.WriteMode) *pack.Package {
	// construct a new package
	ipkg := pack.New().WithSchema(c.idxSchema).WithMaxRows(pkg.Cap())

	// use a new allocated hash block
	hashBlock := block.New(block.BlockUint64, pkg.Len())
	ipkg.WithBlock(0, hashBlock)

	// relink other source blocks in index schema order
	for i, v := range c.srcBlocks {
		b := pkg.Block(v)
		b.Ref()
		ipkg.WithBlock(i+1, b)
	}

	// hash construction is more expensive, we need to push multiple values from different
	// bit-width data blocks through our hash function
	var x [8]byte
	hasher := xxhash64.New()
	u64 := hashBlock.Uint64()
	sel := pkg.Selected()
	for i, l := 0, pkg.Len(); i < l; i++ {
		// produce hash only when needed
		if mode == pack.WriteModeIncludeSelected {
			if len(sel) == 0 || i < int(sel[0]) {
				u64.Append(0)
				continue
			}
			sel = sel[1:]
		} else if mode == pack.WriteModeExcludeSelected {
			if len(sel) > 0 && i == int(sel[0]) {
				u64.Append(0)
				sel = sel[1:]
				continue
			}
		}

		hasher.Reset()

		// assemble hash from multiple data blocks
		for _, n := range c.hashBlocks {
			b := pkg.Block(n)
			switch b.Type() {
			case block.BlockInt64, block.BlockUint64, block.BlockFloat64:
				LE.PutUint64(x[:], b.Uint64().Get(i))
				hasher.Write(x[:])
			case block.BlockInt32, block.BlockUint32, block.BlockFloat32:
				LE.PutUint32(x[:], b.Uint32().Get(i))
				hasher.Write(x[:4])
			case block.BlockInt16, block.BlockUint16:
				LE.PutUint16(x[:], b.Uint16().Get(i))
				hasher.Write(x[:2])
			case block.BlockInt8, block.BlockUint8:
				hasher.Write([]byte{b.Uint8().Get(i)})
			case block.BlockBool:
				hasher.Write([]byte{util.Bool2byte(b.Bool().Get(i))})
			case block.BlockBytes:
				hasher.Write(b.Bytes().Get(i))
			case block.BlockInt128:
				hasher.Write(b.Int128().Get(i).Bytes())
			case block.BlockInt256:
				hasher.Write(b.Int256().Get(i).Bytes())
			}
		}
		u64.Append(hasher.Sum64())
	}

	ipkg.UpdateLen()
	return ipkg
}

func (c *CompositeHashConverter) QueryKeys(node *filter.Node) []uint64 {
	// identify eligible conditions for constructing multi-field lookups
	eq := make(map[string]*filter.Node) // all equal child conditions
	for _, child := range node.Children {
		if child.Filter.Mode == types.FilterModeEqual {
			eq[child.Filter.Name] = child
		}
	}

	// try combine multiple AND leaf conditions into longer index key,
	// all index fields must be available
	buf := new(bytes.Buffer)
	nfields := c.srcSchema.NumFields()
	for _, field := range c.srcSchema.Fields()[:nfields-1] {
		name := field.Name()
		node, ok := eq[name]
		if !ok {
			// empty result if we cannot build a hash from all index fields
			return nil
		}
		field.Encode(buf, node.Filter.Value, LE)
		// set skip flags signalling this condition has been processed
		node.Skip = true
		delete(eq, name)
	}

	// create single hash key from composite EQ conditions
	return []uint64{xxhash64.Sum64(buf.Bytes())}
}

func (*CompositeHashConverter) QueryNode(_ *filter.Node) *filter.Node {
	// unused (range index scans only)
	return nil
}
