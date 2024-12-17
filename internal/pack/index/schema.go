// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// type IndexRecord struct {
//  Key uint64 // hash key, i.e. xxh(value)
//  Pk  uint64 // OID of table entry
// }

func convertSchema(s *schema.Schema, typ types.IndexType) (ixs *schema.Schema, hf hashFunc, err error) {
	hf = genNoopKey

	// requires at least two fields
	if s.NumFields() < 2 {
		err = fmt.Errorf("pack index requires at least two schema fields")
		return
	}
	// last field must be primary key
	pkf := s.Fields()[s.NumFields()-1]
	if !pkf.Is(types.FieldFlagPrimary) {
		err = fmt.Errorf("last schema field must be primary key")
		return
	}

	switch typ {
	case types.IndexTypeHash:
		// supports single source column of any type
		// first column: hash value (uint64)
		// second column: pk
		if s.NumFields() > 2 {
			err = fmt.Errorf("too many schema fields for hash index")
		} else {
			hf = genHashKey64
			ixs = schema.NewSchema().
				WithName(s.Name()).
				WithVersion(s.Version()).
				WithField(schema.NewField(types.FieldTypeUint64).WithName("hash")).
				WithField(pkf).
				Finalize()
		}

	case types.IndexTypeInt:
		// supports single source column of integer type only
		// first column: integer value (same type/width) as source
		// second column: pk
		if s.NumFields() > 2 {
			err = fmt.Errorf("too many schema columns for integer index")
		} else {
			f, _ := s.FieldByIndex(0)
			switch f.Type() {
			case types.FieldTypeDatetime,
				types.FieldTypeInt64,
				types.FieldTypeUint64,
				types.FieldTypeInt32,
				types.FieldTypeInt16,
				types.FieldTypeInt8,
				types.FieldTypeUint32,
				types.FieldTypeUint16,
				types.FieldTypeUint8:

				// convert shorter integers to u64
				hf = makeKeyGen(f.WireSize())
				ixs = schema.NewSchema().
					WithName(s.Name()).
					WithVersion(s.Version()).
					WithField(schema.NewField(types.FieldTypeUint64).WithName("int")).
					WithField(pkf).
					Finalize()

			default:
				err = fmt.Errorf("invalid field type %s for integer index", f.Type())
			}

		}

	case types.IndexTypeComposite:
		// supports any number of source columns >= 1
		// first column: hash value (uint32/uint64)
		// second column: pk
		hf = genHashKey64
		ixs = schema.NewSchema().
			WithName(s.Name()).
			WithVersion(s.Version()).
			WithField(schema.NewField(types.FieldTypeUint64).WithName("hash")).
			WithField(pkf).
			Finalize()

	default:
		// unsupported
		err = fmt.Errorf("unsupported pack index type %q", typ)
	}
	return
}
