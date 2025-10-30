// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type indexTest struct {
	name      string
	build     func() (*Schema, error)
	idxnames  []string
	idxfields []string
	idxextra  []string
	idxtyps   []IndexType
	iserr     bool
}

type IntegerIndexWithExtra struct {
	BaseModel
	Int1 int64 `knox:"i62"`
	Int2 int64 `knox:"i64,index=int,extra=i62"`
}

type BadIntegerIndexWithFields struct {
	BaseModel
	Int1 int64 `knox:"i62"`
	Int2 int64 `knox:"i64,index=int,fields=i62"` // illegal
}

type CompositeIndex struct {
	BaseModel
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i64+i66"`
}

type DoubleCompositeIndex struct {
	BaseModel
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i65"`
	Int3 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i64+i65"`
	_    struct{} `knox:"c2,index=composite,fields=i65+i66,extra=i64+i66"`
}

type BadCompositeIndexMissingField struct {
	BaseModel
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i62+i66"` // illegal
}

type BadCompositeIndexDuplicateField struct {
	BaseModel
	Int1 int64    `knox:"i64"`
	Int2 int64    `knox:"i66"`
	_    struct{} `knox:"c1,index=composite,fields=i64+i64+i66"` // illegal
}

type BadCompositeIndexDuplicateExtraField struct {
	BaseModel
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
		build:     GenericSchema[HashIndex],
		idxnames:  []string{"id_index", "hash_index"},
		idxfields: []string{"id", "hash"},
		idxextra:  []string{"", ""},
		idxtyps:   []IndexType{I_PK, I_HASH},
	},
	{
		name:      "integer index",
		build:     GenericSchema[IntegerIndex],
		idxnames:  []string{"id_index", "i64_index"},
		idxfields: []string{"id", "i64"},
		idxextra:  []string{"", ""},
		idxtyps:   []IndexType{I_PK, I_INT},
	},
	{
		name:      "integer index with extra",
		build:     GenericSchema[IntegerIndexWithExtra],
		idxnames:  []string{"id_index", "i64_index"},
		idxfields: []string{"id", "i64"},
		idxextra:  []string{"", "i62"},
		idxtyps:   []IndexType{I_PK, I_INT},
	},
	{
		name:      "composite index",
		build:     GenericSchema[CompositeIndex],
		idxnames:  []string{"id_index", "c1"},
		idxfields: []string{"id", "i64,i66"},
		idxextra:  []string{"", ""},
		idxtyps:   []IndexType{I_PK, I_COMPOSITE},
	},
	{
		name:      "double composite index",
		build:     GenericSchema[DoubleCompositeIndex],
		idxnames:  []string{"id_index", "c1", "c2"},
		idxfields: []string{"id", "i64,i65", "i65,i66"},
		idxextra:  []string{"", "", "i64,i66"},
		idxtyps:   []IndexType{I_PK, I_COMPOSITE, I_COMPOSITE},
	},

	// errors
	{
		name:  "invalid integer index with fields",
		build: GenericSchema[BadIntegerIndexWithFields],
		iserr: true,
	},
	{
		name:  "invalid index type",
		build: GenericSchema[InvalidIndexType],
		iserr: true,
	},
	{
		name:  "invalid index field type",
		build: GenericSchema[InvalidIndexFieldType],
		iserr: true,
	},
	{
		name:  "invalid composite index with missing field",
		build: GenericSchema[BadCompositeIndexMissingField],
		iserr: true,
	},
	{
		name:  "invalid composite index with duplicate field",
		build: GenericSchema[BadCompositeIndexDuplicateField],
		iserr: true,
	},
	{
		name:  "invalid composite index with duplicate extra field",
		build: GenericSchema[BadCompositeIndexDuplicateExtraField],
		iserr: true,
	},
}

func TestIndexParsing(t *testing.T) {
	for _, c := range indexTestCases {
		t.Run(c.name, func(t *testing.T) {
			// check test data consistency
			require.NotNil(t, c.build, "must define GenericSchema[T] function in testcase")
			require.Equal(t, len(c.idxfields), len(c.idxextra), "must have equal number of idx and extra field definitions")
			require.Equal(t, len(c.idxfields), len(c.idxtyps), "must have equal number of idx and type definitions")
			require.Equal(t, len(c.idxfields), len(c.idxnames), "must have equal number of idx and name definitions")
			// build the schema
			s, err := c.build()
			if c.iserr {
				require.Error(t, err)
				t.Log(err)
				return
			} else {
				require.NoError(t, err)
				require.NoError(t, s.Validate())
			}

			// check index detection
			// for _, v := range s.Indexes {
			// 	t.Logf("%s: %s %#v, %#v", v.Name, v.Type, v.Fields, v.Extra)
			// }

			require.Equal(t, len(s.Indexes), len(c.idxfields), "bad index count")
			for i, ifx := range c.idxfields {
				iex := c.idxextra[i]
				var idxfields, extrafields []string
				if len(ifx) > 0 {
					idxfields = strings.Split(ifx, ",")
				}
				if len(iex) > 0 {
					extrafields = strings.Split(iex, ",")
				}
				idx := s.Indexes[i]
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

func TestIndexValidation(t *testing.T) {
	testCases := []struct {
		name      string
		build     *Builder
		expectErr bool
	}{
		{
			name:      "Valid integer index",
			build:     NewBuilder().Int64("test_field").IntIndex("test_field"),
			expectErr: false,
		},
		{
			name:      "Valid PK index",
			build:     NewBuilder().Uint64("pk", Primary()).PkIndex(),
			expectErr: false,
		},
		{
			name:      "Valid hash index",
			build:     NewBuilder().String("hello").HashIndex("hello"),
			expectErr: false,
		},
		{
			name: "Valid composite index",
			build: NewBuilder().
				String("hello").
				String("world").
				CompositeIndex("hello_index",
					IndexField("hello"),
					IndexField("world"),
					ExtraField("hello"),
				),
			expectErr: false,
		},
		{
			name:      "Invalid index type",
			build:     NewBuilder().Int32("i32").AddIndex("i", types.IndexType(100), IndexField("i32")),
			expectErr: true,
		},
		{
			name:      "Invalid int index on non int field",
			build:     NewBuilder().String("s").IntIndex("s"),
			expectErr: true,
		},
		{
			name:      "Pk index on non-pk field",
			build:     NewBuilder().String("hello").AddIndex("", types.IndexTypePk, IndexField("hello")),
			expectErr: true,
		},
		{
			name:      "Index field does not exist",
			build:     NewBuilder().String("hello").HashIndex("notthere"),
			expectErr: true,
		},
		{
			name:      "Composite index on one field only",
			build:     NewBuilder().String("hello").CompositeIndex("", IndexField("hello")),
			expectErr: true,
		},
		{
			name:      "Composite with duplicate fields",
			build:     NewBuilder().String("s1").String("s2").CompositeIndex("", IndexField("s1"), IndexField("s1")),
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.build.WithMeta(true).Finalize().Validate()
			if tc.expectErr {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}

			// check generated index schemas
			for _, ixs := range tc.build.Schema().Indexes {
				s, err := ixs.IndexSchema()
				require.NoError(t, err)
				require.NoError(t, s.Validate())

				s, err = ixs.StorageSchema()
				require.NoError(t, err)
				require.NoError(t, s.Validate())
			}
		})
	}
}
