// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package yaml

import (
	"encoding/hex"
	"fmt"
	"time"

	"blockwatch.cc/knoxdb/pkg/schema"
	"go.yaml.in/yaml/v4"
)

const (
	UnionTypeKey  = "type"
	UnionValueKey = "value"

	NullTag      = "!!null"
	MapTag       = "!!map"
	SeqTag       = "!!seq"
	BoolTag      = "!!bool"
	StrTag       = "!!str"
	IntTag       = "!!int"
	FloatTag     = "!!float"
	TimestampTag = "!!timestamp"
)

// RecordToNode converts a schema record into a yaml.Node tree
// following the schema's type and nesting. Useful for producing
// human-readable output and config files.
func RecordToNode(view *schema.View) (*yaml.Node, error) {
	return buildStructNode(view, 0)
}

func buildStructNode(view *schema.View, ofs int) (*yaml.Node, error) {
	node := mapNode(view.Schema().NumFields() * 2)
	for i, f := range view.Schema().FieldsSeq() {
		if i < ofs {
			continue
		}

		keyNode := strNode(f.Basename())

		valueNode, err := buildFieldNode(f, view, i)
		if err != nil {
			return nil, err
		}

		node.Content = append(node.Content, keyNode, valueNode)
	}

	return node, nil
}

func buildFieldNode(f *schema.Field, view *schema.View, index int) (*yaml.Node, error) {
	switch f.Type {
	case schema.Timestamp:
		return timeNode(view.Timestamp(index).Format(time.RFC3339Nano)), nil
	case schema.Duration:
		return strNode(view.Duration(index).String()), nil
	case schema.Date:
		return strNode(schema.TIME_SCALE_DAY.Format(view.Date(index))), nil
	case schema.Time:
		return strNode(schema.TIME_SCALE_SECOND.FormatTime(view.Time(index))), nil
	case schema.Uint64:
		return intNode(view.Uint64(index)), nil
	case schema.Uint32:
		return intNode(view.Uint32(index)), nil
	case schema.Uint16:
		return intNode(view.Uint16(index)), nil
	case schema.Uint8:
		return intNode(view.Uint8(index)), nil
	case schema.Int64:
		return intNode(view.Int64(index)), nil
	case schema.Int32:
		return intNode(view.Int32(index)), nil
	case schema.Int16:
		return intNode(view.Int16(index)), nil
	case schema.Int8:
		return intNode(view.Int8(index)), nil
	case schema.Boolean:
		return boolNode(view.Bool(index)), nil
	case schema.Float64:
		return floatNode(view.Float64(index)), nil
	case schema.Float32:
		return floatNode(view.Float32(index)), nil
	case schema.Int128:
		return strNode(view.Int128(index)), nil
	case schema.Int256:
		return strNode(view.Int256(index)), nil
	case schema.Decimal32:
		return strNode(view.Decimal32(index)), nil
	case schema.Decimal64:
		return strNode(view.Decimal64(index)), nil
	case schema.Decimal128:
		return strNode(view.Decimal128(index)), nil
	case schema.Decimal256:
		return strNode(view.Decimal256(index)), nil
	case schema.String:
		return strNode(view.String(index)), nil
	case schema.Text:
		return strNode(view.Text(index)), nil
	case schema.Bytes:
		return strNode(hex.EncodeToString(view.Bytes(index))), nil
	case schema.Binary:
		return strNode(hex.EncodeToString(view.Binary(index))), nil
	case schema.Enum:
		return strNode(view.Enum(index)), nil
	case schema.Bigint:
		return strNode(view.Bigint(index).String()), nil
	case schema.Union:
		return buildUnionNode(view.Union(index)), nil
	case schema.List:
		return buildListNode(view, index)
	case schema.Map:
		return buildMapNode(view, index)
	case schema.Variant:
		return buildVariantNode(view, index)
	default:
		return nil, fmt.Errorf("build: unsupported type %s", f.Type)
	}
}

func buildListNode(view *schema.View, index int) (*yaml.Node, error) {
	node := seqNode()
	for _, vv := range view.List(index) {
		var (
			elem *yaml.Node
			err  error
		)
		if vv.Schema().NumFields() == 1 {
			elem, err = buildFieldNode(vv.Schema().Field(0), vv, 0)
		} else {
			elem, err = buildStructNode(vv, 0)
		}
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, elem)
	}
	return node, nil
}

func buildMapNode(view *schema.View, index int) (*yaml.Node, error) {
	node := mapNode()
	var (
		keyField, valField *schema.Field
		numFields          int
	)
	for _, vv := range view.Map(index) {
		if keyField == nil {
			keyField = vv.Schema().Field(0)
			valField = vv.Schema().Field(1)
			numFields = vv.Schema().NumFields()
		}
		keyNode, err := buildFieldNode(keyField, vv, 0)
		if err != nil {
			return nil, err
		}
		var valNode *yaml.Node
		if numFields == 2 {
			valNode, err = buildFieldNode(valField, vv, 1)
		} else {
			valNode, err = buildStructNode(vv, 1)
		}
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, keyNode, valNode)
	}

	return node, nil
}

func buildUnionNode(u schema.UnionValue) *yaml.Node {
	var cnode *yaml.Node
	switch u.Type() {
	case schema.Int64, schema.Int32, schema.Int16, schema.Int8,
		schema.Uint64, schema.Uint32, schema.Uint16, schema.Uint8:
		cnode = intNode(u.Value())
	case schema.Timestamp:
		cnode = timeNode(u.Timestamp().Format(time.RFC3339Nano))
	case schema.Boolean:
		cnode = boolNode(u.Value())
	case schema.Float64, schema.Float32:
		cnode = floatNode(u.Value())
	default:
		cnode = strNode(u.Value())
	}
	n := mapNode(4)
	n.Content = append(n.Content,
		strNode(UnionTypeKey), strNode(u.Type().String()),
		strNode(UnionValueKey), cnode,
	)
	return n
}

func buildVariantNode(view *schema.View, index int) (*yaml.Node, error) {
	// recurse into variant
	vv, _ := view.Variant(index, nil)
	caseSchema := vv.Schema()
	caseName := caseSchema.Name

	// Create mapping with one key (the case name)
	node := &yaml.Node{
		Kind: yaml.MappingNode,
		Tag:  MapTag,
		Content: []*yaml.Node{
			strNode(caseName),
		},
	}

	// Recursively build the case content
	valueNode, err := buildStructNode(vv, 0)
	if err != nil {
		return nil, err
	}
	node.Content = append(node.Content, valueNode)

	return node, nil
}

// Helpers for creating scalar nodes
func strNode(v any) *yaml.Node   { return yamlNode(StrTag, v) }
func intNode(v any) *yaml.Node   { return yamlNode(IntTag, v) }
func floatNode(v any) *yaml.Node { return yamlNode(FloatTag, v) }
func boolNode(v any) *yaml.Node  { return yamlNode(BoolTag, v) }
func timeNode(v any) *yaml.Node  { return yamlNode(TimestampTag, v) }

func mapNode(n ...int) *yaml.Node {
	l := 4
	if len(n) > 0 {
		l = n[0]
	}
	return &yaml.Node{
		Kind:    yaml.MappingNode,
		Tag:     MapTag,
		Content: make([]*yaml.Node, 0, max(l, 4)),
	}
}

func seqNode(n ...int) *yaml.Node {
	l := 4
	if len(n) > 0 {
		l = n[0]
	}
	return &yaml.Node{
		Kind:    yaml.SequenceNode,
		Tag:     SeqTag,
		Content: make([]*yaml.Node, 0, max(l, 4)),
	}
}

func yamlNode(tag string, val any) *yaml.Node {
	return &yaml.Node{
		Kind:  yaml.ScalarNode,
		Tag:   tag,
		Value: fmt.Sprintf("%v", val),
	}
}
