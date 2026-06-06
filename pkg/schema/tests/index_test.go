// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema_tests

import (
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexValidation(t *testing.T) {
	testCases := []struct {
		name      string
		index     *schema.IndexSchema
		expectErr bool
	}{
		{
			name: "Valid integer index",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.Int64, schema.WithName("test_field")),
				}),
				schema.IntegerIndex,
				schema.WithIndexField("test_field"),
			),
			expectErr: false,
		},
		{
			name: "Valid PK index",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.Uint64, schema.WithName("pk"), schema.WithFlags(schema.FlagPrimary)),
				}),
				schema.PrimaryKeyIndex,
			),
			expectErr: false,
		},
		{
			name: "Valid hash index",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.String, schema.WithName("hello")),
				}),
				schema.HashIndex,
				schema.WithIndexField("hello"),
			),
			expectErr: false,
		},
		{
			name: "Valid composite index",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.String, schema.WithName("hello")),
					schema.FieldOf(schema.String, schema.WithName("world")),
				}),
				schema.CompositeIndex,
				schema.WithIndexField("hello"),
				schema.WithIndexField("world"),
				schema.WithExtraField("hello"),
			),
			expectErr: false,
		},
		{
			name: "Invalid index type",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.Int32, schema.WithName("i32")),
				}),
				schema.IndexType(100),
				schema.WithIndexField("i32"),
			),
			expectErr: true,
		},
		{
			name: "Invalid int index on non int field",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.String, schema.WithName("s")),
				}),
				schema.IntegerIndex,
				schema.WithIndexField("s"),
			),
			expectErr: true,
		},
		{
			name: "Pk index on non-pk field",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.String, schema.WithName("hello")),
				}),
				schema.PrimaryKeyIndex,
				schema.WithIndexField("hello"),
			),
			expectErr: true,
		},
		{
			name: "Index field does not exist",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.String, schema.WithName("hello")),
				}),
				schema.HashIndex,
				schema.WithIndexField("notthere"),
			),
			expectErr: true,
		},
		{
			name: "Composite index on one field only",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.String, schema.WithName("hello")),
				}),
				schema.CompositeIndex,
				schema.WithIndexField("hello"),
			),
			expectErr: true,
		},
		{
			name: "Composite with duplicate fields",
			index: schema.IndexOf(
				schema.SchemaOf([]*schema.Field{
					schema.FieldOf(schema.String, schema.WithName("s1")),
					schema.FieldOf(schema.String, schema.WithName("s2")),
				}),
				schema.CompositeIndex,
				schema.WithIndexField("s1"),
				schema.WithIndexField("s1"),
			),
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectErr {
				assert.Error(t, tc.index.Validate())
				return
			} else {
				assert.NoError(t, tc.index.Validate())
			}

			// check generated index schemas
			s, err := tc.index.IndexSchema()
			require.NoError(t, err)
			require.NoError(t, s.Validate())
		})
	}
}
