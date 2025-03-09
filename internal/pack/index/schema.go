// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// convertSchema constructs a physical index schema and a converter for an index.
func convertSchema(s, ts *schema.Schema, typ types.IndexType) (*schema.Schema, Converter, error) {
	// requires at least two fields
	if s.NumFields() < 2 {
		return nil, nil, fmt.Errorf("pack index requires at least two schema fields")
	}

	// schema must be child of table schema
	if err := ts.CanSelect(s); err != nil {
		return nil, nil, err
	}

	// last field must be row id
	pkf := s.Fields()[s.NumFields()-1]
	if pkf.Id() != schema.MetaRid {
		return nil, nil, fmt.Errorf("last schema field must be row id")
	}

	switch typ {
	case types.IndexTypeHash:
		// supports single source column of any type
		// first column: hash value (uint64)
		// second column: rid
		if s.NumFields() > 2 {
			return nil, nil, fmt.Errorf("too many schema fields for hash index")
		}
		ixs := schema.NewSchema().
			WithName(s.Name()).
			WithVersion(s.Version()).
			WithField(schema.NewField(types.FieldTypeUint64).WithName("hash")).
			WithField(pkf).
			Finalize()
		c := &SimpleHashConverter{
			schema: s,
		}
		for n, f := range s.Exported() {
			i, _ := ts.FieldIndexById(f.Id)
			if n == 0 {
				c.hashBlock = i
			} else {
				c.srcBlocks = append(c.srcBlocks, i)
			}
		}
		return ixs, c, nil

	case types.IndexTypeInt:
		// supports single source column of integer type only
		// first column: integer value (same type/width) as source
		// second column: rid
		if s.NumFields() > 2 {
			return nil, nil, fmt.Errorf("too many schema columns for integer index")
		}

		f, _ := s.FieldByIndex(0)
		switch f.Type() {
		default:
			return nil, nil, fmt.Errorf("invalid field type %s for integer index", f.Type())
		case types.FieldTypeDatetime,
			types.FieldTypeInt64,
			types.FieldTypeUint64,
			types.FieldTypeInt32,
			types.FieldTypeInt16,
			types.FieldTypeInt8,
			types.FieldTypeUint32,
			types.FieldTypeUint16,
			types.FieldTypeUint8:

			ixs := schema.NewSchema().
				WithName(s.Name()).
				WithVersion(s.Version()).
				WithField(schema.NewField(types.FieldTypeUint64).WithName("int")).
				WithField(pkf).
				Finalize()

			c := &RelinkConverter{
				schema: s,
			}
			for _, f := range s.Exported() {
				i, _ := ts.FieldIndexById(f.Id)
				c.srcBlocks = append(c.srcBlocks, i)
			}
			return ixs, c, nil
		}

	case types.IndexTypeComposite:
		// supports any number of source columns >= 1
		// first column: hash value (uint64)
		// second column: rid
		ixs := schema.NewSchema().
			WithName(s.Name()).
			WithVersion(s.Version()).
			WithField(schema.NewField(types.FieldTypeUint64).WithName("hash")).
			WithField(pkf).
			Finalize()

		c := &CompositeHashConverter{
			schema: s,
		}
		for _, f := range s.Exported() {
			i, _ := ts.FieldIndexById(f.Id)
			if f.Index == types.IndexTypeComposite {
				c.hashBlocks = append(c.hashBlocks, i)
			} else {
				c.srcBlocks = append(c.srcBlocks, i)
			}
		}
		return ixs, c, nil

	default:
		// unsupported
		return nil, nil, fmt.Errorf("unsupported pack index type %q", typ)
	}
}
