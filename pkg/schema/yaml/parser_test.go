// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package yaml

import (
	"encoding/hex"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/encode"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	tests "blockwatch.cc/knoxdb/pkg/schema/tests"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

var enums *enum.Registry

func TestMain(m *testing.M) {
	enums = tests.RegisterEnums()
	m.Run()
}

type Config struct {
	Config yaml.Node `yaml:"config"`
}

func TestParser(t *testing.T) {
	// encodable types
	t.Run("prim", func(t *testing.T) {
		for _, val := range []any{
			tests.NewAllTypes(42),
		} {
			s, err := reflect.SchemaOf(val, schema.Enums(enums))
			require.NoError(t, err)
			t.Log(s)

			// export schema
			exp, err := ExportSchema(s)
			require.NoError(t, err)
			t.Log(string(exp))

			// encode data (golden version)
			enc := encode.NewEncoder(s)
			buf := s.NewBuffer(1)
			require.NoError(t, enc.Encode(buf, val))

			// produce YAML (alternative 2: via builder)
			v := schema.NewView(s).Reset(buf.Bytes())
			ynode, err := RecordToNode(v)
			require.NoError(t, err)

			ybuf, err := yaml.Marshal(&Config{Config: *ynode})
			require.NoError(t, err)
			t.Log(string(ybuf))

			// decode from yaml buffer into node
			var dec Config
			require.NoError(t, yaml.Unmarshal(ybuf, &dec))

			// convert back to record
			rec, err := NodeToRecord(s, &dec.Config)
			require.NoError(t, err)

			// compare against golden encoded version
			require.Equal(t, buf.Bytes(), rec)
			t.Logf("%T OK", val)
		}
	})

	t.Run("lists", func(t *testing.T) {
		for _, val := range []any{
			tests.NewListFields(),
			tests.NewListInListFields(),
			tests.NewListInStructInListFields(),
		} {
			s, err := reflect.SchemaOf(val, schema.Enums(enums))
			require.NoError(t, err)
			t.Log(s)

			// export schema
			exp, err := ExportSchema(s)
			require.NoError(t, err)
			t.Log(string(exp))

			// encode data (golden version)
			enc := encode.NewEncoder(s)
			buf := s.NewBuffer(1)
			require.NoError(t, enc.Encode(buf, val))

			// produce YAML (alternative 2: via builder)
			v := schema.NewView(s).Reset(buf.Bytes())
			ynode, err := RecordToNode(v)
			require.NoError(t, err)

			ybuf, err := yaml.Marshal(&Config{Config: *ynode})
			require.NoError(t, err)
			t.Log(string(ybuf))

			// decode from yaml buffer into node
			var dec Config
			require.NoError(t, yaml.Unmarshal(ybuf, &dec))

			// convert back to record
			rec, err := NodeToRecord(s, &dec.Config)
			require.NoError(t, err)

			// compare against golden encoded version
			require.Equal(t, buf.Bytes(), rec)
			t.Logf("%T OK", val)
		}
	})

	// map types implementing MarshalSchema
	t.Run("maps", func(t *testing.T) {
		for _, val := range []any{
			tests.NewPrimMapRecord(),
			tests.NewUnionMapRecord(),
			tests.NewMapFields(),
		} {
			s, err := reflect.SchemaOf(val, schema.Enums(enums))
			require.NoError(t, err)
			t.Log(s)

			// export schema
			exp, err := ExportSchema(s)
			require.NoError(t, err)
			t.Log(string(exp))

			// encode data (golden version)
			w := schema.NewWriter(s, nil)
			require.NoError(t, w.Append(val))
			buf := w.Bytes()

			// produce YAML (alternative 2: via builder)
			v := schema.NewView(s).Reset(buf)
			ynode, err := RecordToNode(v)
			require.NoError(t, err)

			ybuf, err := yaml.Marshal(&Config{Config: *ynode})
			require.NoError(t, err)
			t.Log(string(ybuf))

			// decode from yaml buffer into node
			var dec Config
			require.NoError(t, yaml.Unmarshal(ybuf, &dec))

			// convert back to record
			rec, err := NodeToRecord(s, &dec.Config)
			require.NoError(t, err)

			// compare against golden encoded version
			require.Equal(t, buf, rec)
			t.Logf("%T OK", val)
		}
	})

	// variant types implementing MarshalSchema
	t.Run("variant", func(t *testing.T) {
		// encode data (golden version)
		s := tests.CustomerT
		w := schema.NewWriter(s, nil)
		val := tests.NewCustomer()
		require.NoError(t, w.Append(val))
		require.True(t, w.Done())
		buf := w.Bytes()

		// export schema
		exp, err := ExportSchema(s)
		require.NoError(t, err)
		t.Log(string(exp))

		// produce YAML (alternative 2: via builder)
		v := schema.NewView(s).Reset(buf)
		ynode, err := RecordToNode(v)
		require.NoError(t, err)

		ybuf, err := yaml.Marshal(&Config{Config: *ynode})
		require.NoError(t, err)
		t.Log(string(ybuf))

		// decode from yaml buffer into node
		var dec Config
		require.NoError(t, yaml.Unmarshal(ybuf, &dec))

		// convert back to record
		rec, err := NodeToRecord(s, &dec.Config)
		require.NoError(t, err)

		// compare against golden encoded version
		require.Equal(t, buf, rec)
		t.Logf("%T OK", val)
	})
}

func TestSkip(t *testing.T) {
	type testCase struct {
		name   string
		yaml   string
		schema *schema.Schema
		val    any
	}
	cases := []testCase{
		// skip primitive fields
		{
			name: "prim",
			yaml: `
config:
    i64: 42
    union:
        type: bool
        value: true
`,
			schema: reflect.MustSchemaFor[tests.AllTypes](schema.Enums(enums)),
			val:    &tests.AllTypes{},
		},
		// skip a lists and element fields
		{
			name: "prim_list",
			yaml: `
config:
    pair_list:
        - k64: 42
`,
			schema: reflect.MustSchemaFor[tests.ListFields](schema.Enums(enums)),
			val:    &tests.ListFields{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Log(string(MustExportSchema(c.schema)))

			// decode from yaml string into node
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(c.yaml), &cfg))

			// convert to record
			rec, err := NodeToRecord(c.schema, &cfg.Config)
			require.NoError(t, err)
			require.NotNil(t, rec)
			require.LessOrEqual(t, c.schema.MinWireSize, len(rec))
			t.Log(hex.Dump(rec))

			// decode buffer
			dec := encode.NewDecoder(c.schema)
			require.NoError(t, dec.Decode(rec, c.val))
			t.Logf("%#v", c.val)
		})
	}
}

func TestErrors(t *testing.T) {
	type testCase struct {
		name   string
		yaml   string
		schema *schema.Schema
	}
	cases := []testCase{
		// string instead of int top-level primitive
		{
			name: "prim",
			yaml: `
config:
    i64: "x"
`,
			schema: reflect.MustSchemaFor[tests.AllTypes](schema.Enums(enums)),
		},
		// wrong time format
		{
			name: "prim",
			yaml: `
config:
    time: "2026-06-16 09:56:30.721987 UTC"
`,
			schema: reflect.MustSchemaFor[tests.AllTypes](schema.Enums(enums)),
		},
		// string instead of int list field
		{
			name: "prim_list",
			yaml: `
config:
    pair_list:
        - k64: "x"
`,
			schema: reflect.MustSchemaFor[tests.ListFields](schema.Enums(enums)),
		},
		// int8 overflow
		{
			name: "int_overflow",
			yaml: `
config:
    i8: 128
`,
			schema: reflect.MustSchemaFor[tests.AllTypes](schema.Enums(enums)),
		},
		// string length overflow
		{
			name: "string_overflow",
			yaml: `
config:
    string: "9530494154739592394941278801817795304941547395923949412788018177953049415473959239494127880181779530494154739592394941278801817795304941547395923949412788018177953049415473959239494127880181779530494154739592394941278801817795304941547395923949412788018177"
`,
			schema: reflect.MustSchemaFor[tests.AllTypes](schema.Enums(enums)),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// decode from yaml string into node
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(c.yaml), &cfg))

			// convert to record
			_, err := NodeToRecord(c.schema, &cfg.Config)
			require.Error(t, err)
			t.Log(err)
		})
	}
}
