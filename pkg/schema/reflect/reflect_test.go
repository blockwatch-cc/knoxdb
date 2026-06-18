// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"github.com/stretchr/testify/require"
)

var (
	enums  *enum.Registry
	myEnum *enum.Dictionary
)

func TestMain(m *testing.M) {
	// prepare enum
	myEnum = enum.NewDictionary("my_enum")
	myEnum.Append("a", "b", "c", "d", "e")

	// create test registry and add enum to registry
	enums = enum.NewRegistry()
	enums.Register(0, myEnum)

	m.Run()
}

func TestSchemaOf(t *testing.T) {
	// make sure registry is empty
	schemaRegistry.Clear()

	// need to attach enum on first call
	s, err := SchemaOf(AllTypes{}, schema.Enums(enums))
	require.NoError(t, err)
	require.NotNil(t, s)

	// second call is served from registry
	s, err = SchemaOf(&AllTypes{})
	require.NoError(t, err)
	require.NotNil(t, s)

	// clear registry again and try again, should error
	schemaRegistry.Clear()
	s, err = SchemaOf(&AllTypes{})
	require.Error(t, err)
	require.Nil(t, s)

	// run with enum registry option should succeed
	s, err = SchemaOf(&AllTypes{}, schema.Enums(enums))
	require.NoError(t, err)
	require.NotNil(t, s)

	s, err = SchemaOf(nil)
	require.Error(t, err)
	require.Nil(t, s)
}

func TestSchemaFor(t *testing.T) {
	s, err := SchemaFor[AllTypes](schema.Enums(enums))
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestSchemaRoundtrip(t *testing.T) {
	// produce a schema from Go type
	s, err := SchemaFor[AllTypes](schema.Enums(enums))
	require.NoError(t, err)
	t.Log(s)

	// produce a dynamic Go reflect.Type from schema
	ty := StructTypeOf(s)
	require.Equal(t, reflect.Struct, ty.Kind())
	require.Equal(t, s.NumFields(), ty.NumField()) // some fields are nested

	// infer schema from the dynamic type
	r, err := SchemaOf(reflect.New(ty).Interface(), schema.Enums(enums))
	require.NoError(t, err)
	t.Log(r)

	// schema hashes must match (field types, order, ids, flags are the same,
	// names don't matter, but fields must be exported/visible Go struct fields)
	require.True(t, s.Equal(r))
}
