// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"reflect"
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type IndexTypes struct {
	I1 uint64   `knox:"i1,pk"`
	I2 int64    `knox:"i2,index=hash"`
	I3 int64    `knox:"i3,index=int"`
	I4 int64    `knox:"i4,index=int,extra=i1+i3"`
	_  struct{} `knox:"idx,index=composite,fields=i1+i2,extra=i4+i3"`
}

type BadIndexType1 struct {
	I int64 `knox:"i,index=invalid"`
}

type BadIndexType2 struct {
	I int64 `knox:"i,index="`
}

type BadIndexFieldType struct {
	B []byte `knox:",index=int"`
}

func TestFieldStructReadIndex(t *testing.T) {
	// error schemas
	_, err := IndexesFor[BadIndexType1]()
	assert.Error(t, err)
	_, err = IndexesFor[BadIndexType2]()
	assert.Error(t, err)
	_, err = IndexesFor[BadIndexFieldType]()
	assert.Error(t, err)

	// good schema
	base := MustSchemaFor[IndexTypes]()
	sTypeOf := reflect.TypeFor[IndexTypes]()
	tests := []struct {
		name   string
		idx    schema.IndexType
		fields []string
		extra  []string
		err    bool
	}{
		{"index_types_i1_index", schema.PrimaryKeyIndex, []string{"i1"}, nil, false},
		{"index_types_i2_index", schema.HashIndex, []string{"i2"}, nil, false},
		{"index_types_i3_index", schema.IntegerIndex, []string{"i3"}, nil, false},
		{"index_types_i4_index", schema.IntegerIndex, []string{"i4"}, []string{"i1", "i3"}, false},
		{"index_types_idx_index", schema.CompositeIndex, []string{"i1", "i2"}, []string{"i4", "i3"}, false},
	}
	for i, tt := range tests {
		sf := sTypeOf.Field(i)
		if !sf.IsExported() || sf.Anonymous || sf.Tag.Get(TAG_NAME) == "-" {
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			is, err := reflectStructFieldForIndex(sf, TAG_NAME, base)
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.name, is.Name)
			assert.Equal(t, tt.idx, is.Type)
			assert.Equal(t, len(tt.fields), len(is.Fields))
			for n, f := range is.Fields {
				assert.Equal(t, tt.fields[n], f.Name)
			}
			assert.Equal(t, len(tt.extra), len(is.Extra))
			for n, f := range is.Extra {
				assert.Equal(t, tt.extra[n], f.Name)
			}
		})
	}
}

type indexTest struct {
	name      string
	build     func(...schema.Option) ([]*schema.IndexSchema, error)
	idxnames  []string
	idxfields []string
	idxextra  []string
	idxtyps   []schema.IndexType
	iserr     bool
}

type HashIndex struct {
	Id   uint64   `knox:"id,pk"`
	Hash [32]byte `knox:"hash,index=hash"`
}

type IntegerIndex struct {
	Id  uint64 `knox:"id,pk"`
	Int int64  `knox:"i64,index=int"`
}

type IntegerIndexWithExtra struct {
	Id   uint64 `knox:"id,pk"`
	Int1 int64  `knox:"i62"`
	Int2 int64  `knox:"i64,index=int,extra=i62"`
}

type BadIntegerIndexWithFields struct {
	Id   uint64 `knox:"id,pk"`
	Int1 int64  `knox:"i62"`
	Int2 int64  `knox:"i64,index=int,fields=i62"` // illegal
}

type CompositeIndex struct {
	Id   uint64   `knox:"id,pk"`
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i64+i66"`
}

type DoubleCompositeIndex struct {
	Id   uint64   `knox:"id,pk"`
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i65"`
	Int3 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i64+i65"`
	_    struct{} `knox:"c2,index=composite,fields=i65+i66,extra=i64+i66"`
}

type BadCompositeIndexMissingField struct {
	Id   uint64   `knox:"id,pk"`
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i62+i66"` // illegal
}

type BadCompositeIndexDuplicateField struct {
	Id   uint64   `knox:"id,pk"`
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i64+i64+i66"` // illegal
}

type BadCompositeIndexDuplicateExtraField struct {
	Id   uint64   `knox:"id,pk"`
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i64+i66,extra=i64+i64+i66"` // illegal
}

var indexTestCases = []indexTest{
	//
	// Index tests
	// -----------------

	// allowed index compisitions
	{
		name:      "hash index",
		build:     IndexesFor[HashIndex],
		idxnames:  []string{"hash_index_id_index", "hash_index_hash_index"},
		idxfields: []string{"id", "hash"},
		idxextra:  []string{"", ""},
		idxtyps:   []schema.IndexType{schema.PrimaryKeyIndex, schema.HashIndex},
	},
	{
		name:      "integer index",
		build:     IndexesFor[IntegerIndex],
		idxnames:  []string{"integer_index_id_index", "integer_index_i64_index"},
		idxfields: []string{"id", "i64"},
		idxextra:  []string{"", ""},
		idxtyps:   []schema.IndexType{schema.PrimaryKeyIndex, schema.IntegerIndex},
	},
	{
		name:      "integer index with extra",
		build:     IndexesFor[IntegerIndexWithExtra],
		idxnames:  []string{"integer_index_with_extra_id_index", "integer_index_with_extra_i64_index"},
		idxfields: []string{"id", "i64"},
		idxextra:  []string{"", "i62"},
		idxtyps:   []schema.IndexType{schema.PrimaryKeyIndex, schema.IntegerIndex},
	},
	{
		name:      "composite index",
		build:     IndexesFor[CompositeIndex],
		idxnames:  []string{"composite_index_id_index", "composite_index_c1"},
		idxfields: []string{"id", "i64,i66"},
		idxextra:  []string{"", ""},
		idxtyps:   []schema.IndexType{schema.PrimaryKeyIndex, schema.CompositeIndex},
	},
	{
		name:      "double composite index",
		build:     IndexesFor[DoubleCompositeIndex],
		idxnames:  []string{"double_composite_index_id_index", "double_composite_index_c1", "double_composite_index_c2"},
		idxfields: []string{"id", "i64,i65", "i65,i66"},
		idxextra:  []string{"", "", "i64,i66"},
		idxtyps: []schema.IndexType{
			schema.PrimaryKeyIndex,
			schema.CompositeIndex,
			schema.CompositeIndex,
		},
	},

	// errors
	{
		name:  "invalid integer index with fields",
		build: IndexesFor[BadIntegerIndexWithFields],
		iserr: true,
	},
	{
		name:  "invalid composite index with missing field",
		build: IndexesFor[BadCompositeIndexMissingField],
		iserr: true,
	},
	{
		name:  "invalid composite index with duplicate field",
		build: IndexesFor[BadCompositeIndexDuplicateField],
		iserr: true,
	},
	{
		name:  "invalid composite index with duplicate extra field",
		build: IndexesFor[BadCompositeIndexDuplicateExtraField],
		iserr: true,
	},
}

func TestIndexParsing(t *testing.T) {
	for _, c := range indexTestCases {
		t.Run(c.name, func(t *testing.T) {
			// check test data consistency
			require.NotNil(t, c.build, "must define reflect.SchemaFor[T] function in testcase")
			require.Equal(t, len(c.idxfields), len(c.idxextra), "must have equal number of idx and extra field definitions")
			require.Equal(t, len(c.idxfields), len(c.idxtyps), "must have equal number of idx and type definitions")
			require.Equal(t, len(c.idxfields), len(c.idxnames), "must have equal number of idx and name definitions")
			// build the schema
			indexes, err := c.build()
			if c.iserr {
				require.Error(t, err)
				t.Log(err)
				return
			} else {
				require.NoError(t, err)
				for _, idx := range indexes {
					require.NoError(t, idx.Validate())
				}
			}

			// check index detection
			// for _, v := range s.Indexes {
			// 	t.Logf("%s: %s %#v, %#v", v.Name, v.Type, v.Fields, v.Extra)
			// }

			require.Equal(t, len(c.idxfields), len(indexes), "bad index count")
			for i, ifx := range c.idxfields {
				iex := c.idxextra[i]
				var idxfields, extrafields []string
				if len(ifx) > 0 {
					idxfields = strings.Split(ifx, ",")
				}
				if len(iex) > 0 {
					extrafields = strings.Split(iex, ",")
				}
				idx := indexes[i]
				require.NotNil(t, idx.Base)
				require.Equal(t, c.idxtyps[i], idx.Type, "type mismatch")
				require.Equal(t, c.idxnames[i], idx.Name, "name mismatch")
				require.Equal(t, len(idxfields), len(idx.Fields), "index fields")
				require.Equal(t, len(extrafields), len(idx.Extra), "extra fields")
				for k, n := range idxfields {
					require.Equal(t, n, idx.Fields[k].Name, "idx field name %d", k)
				}
				for k, n := range extrafields {
					require.Equal(t, n, idx.Extra[k].Name, "extra field name %d", k)
				}
			}
		})
	}
}
