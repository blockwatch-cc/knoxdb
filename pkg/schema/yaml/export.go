// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package yaml

import (
	"blockwatch.cc/knoxdb/pkg/schema"
	"go.yaml.in/yaml/v4"
)

// ExportSchema writes a schema definition to YAML
//
// schema:
//
//	name:           # schema name
//	version:        # schema version
//	hash:           # schema hash
//	min_size:       # min record size in bytes
//	is_fixed:       # true when schema is fixed size (optional)
//	fields:         # list of fields (excluding metadata)
//	- id:           # field id
//	  name:         # field_name
//	  type:         # field type
//	  flags:        # field flags (optional)
//	  filter:       # field filter type (optional)
//	  compress:     # field compression type (optional)
//	  element_type: # list element type (list only, optional)
//	  key_type:     # map key type (map only, optional)
//	  value_type:   # map value type (map only, optional)
//	  cases:        # variant case type list (variant only, optional)
//	  enums:        # enum value list (enum only, optional)
func ExportSchema(s *schema.Schema) ([]byte, error) {
	node := mapNode()
	node.Content = append(node.Content,
		strNode("schema"), ExportSchemaNode(s),
	)
	return yaml.Marshal(node)
}

func MustExportSchema(s *schema.Schema) []byte {
	buf, err := ExportSchema(s)
	if err != nil {
		panic(err)
	}
	return buf
}

func ExportSchemaNode(s *schema.Schema) *yaml.Node {
	return buildSchema(s)
}

func buildSchema(s *schema.Schema) *yaml.Node {
	// schema info
	node := mapNode(12)
	node.Content = append(node.Content,
		strNode("name"), strNode(s.Name),
		strNode("version"), intNode(s.Version),
		strNode("hash"), intNode(s.Hash),
		strNode("min_size"), intNode(s.MinWireSize),
	)

	// optional fixed
	if s.IsFixedSize {
		node.Content = append(node.Content, strNode("is_fixed"), boolNode(s.IsFixedSize))
	}

	// field info
	fields := seqNode(s.NumFields())
	for _, f := range s.TopFields() {
		fields.Content = append(fields.Content, buildField(f))
	}
	node.Content = append(node.Content, strNode("fields"), fields)

	return node
}

func buildField(f *schema.Field) *yaml.Node {
	node := mapNode(16)
	node.Content = append(node.Content,
		strNode("id"), intNode(f.Id),
		strNode("name"), strNode(f.Basename()),
		strNode("type"), strNode(f.Typename()),
	)

	// optionals
	if f.Flags > 0 {
		node.Content = append(node.Content, strNode("flags"), strNode(f.Flags))
	}
	if f.Filter > 0 {
		node.Content = append(node.Content, strNode("filter"), strNode(f.Filter))
	}
	if f.Compress > 0 {
		node.Content = append(node.Content, strNode("compress"), strNode(f.Compress))
	}

	// embed
	switch f.Type {
	case schema.List:
		if len(f.Child.Fields) > 1 {
			node.Content = append(node.Content,
				strNode("element_type"), buildSchema(f.ValueType()),
			)
		}
	case schema.Map:
		if f.Child.NumFields() > 2 {
			node.Content = append(node.Content,
				strNode("key_type"), strNode(f.Child.Fields[0].Typename()),
				strNode("value_type"), buildSchema(f.ValueType()),
			)
		}
	case schema.Variant:
		cases := mapNode(f.NumCases())
		for _, cs := range *f.Cases {
			cases.Content = append(cases.Content, strNode(cs.Name), buildSchema(cs))
		}
		node.Content = append(node.Content, strNode("cases"), cases)
	case schema.Enum:
		if f.Enum.Len() > 0 {
			vals := seqNode(f.Enum.Len())
			for v := range f.Enum.Values() {
				vals.Content = append(vals.Content, strNode(v))
			}
			node.Content = append(node.Content, strNode("values"), vals)
		}
	}

	return node
}
