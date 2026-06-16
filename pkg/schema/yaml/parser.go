// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package yaml

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/parse"
	"go.yaml.in/yaml/v4"
)

var (
	ErrIncompleteRecord = errors.New("parse: incomplete record")
)

// NodeToRecord converts a generic schema-compliant YAML value to
// an encoded schema value record and fills missing fields with zero.
// Used to read partial YAML into generic types like configuration
// options without access to a Go struct type.
func NodeToRecord(s *schema.Schema, root *yaml.Node) ([]byte, error) {
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		root = root.Content[0]
	}

	buf := s.NewBuffer(1)
	w := schema.NewWriter(s, buf)

	if err := parseStruct(w, s, root); err != nil {
		return nil, fmt.Errorf("%s.%v", s.Name, err)
	}

	if !w.Done() {
		return nil, ErrIncompleteRecord
	}
	return w.Bytes(), nil
}

func parseStruct(w *schema.Writer, s *schema.Schema, node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("expected mapping, got %s", nodeKind(node))
	}

	yamlMap := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		yamlMap[node.Content[i].Value] = node.Content[i+1]
	}

	// walk all fields at current nesting level (use the passed-in schema
	// rather than Writer.Field() as the latter is ambiguous when the
	// current destination field is nested)
	for _, f := range s.FieldsSeq() {
		valNode, exists := yamlMap[f.Basename()]

		// Missing field, write zero value
		if !exists {
			if err := w.Skip(); err != nil {
				return fmt.Errorf("%s: %w", f.Name, err)
			}
			continue
		}

		if err := parseField(w, valNode); err != nil {
			// forward nested errors which already contain line info
			if strings.Contains(err.Error(), "at line") {
				return err
			}
			return fmt.Errorf("%s: %w at line %d:%d", f.Name, err, valNode.Line, valNode.Column)
		}

		delete(yamlMap, f.Basename())
	}

	// reject unknown fields
	for name, node := range yamlMap {
		return fmt.Errorf("%s: unknown field at line %d:%d (not in schema)", name, node.Line, node.Column)
	}
	return nil
}

func parseField(w *schema.Writer, node *yaml.Node) error {
	f := w.Field()
	var err error
	switch f.Type {
	case schema.Bytes:
		err = parseBytes(w, node)
	case schema.List:
		err = parseList(w, node)
	case schema.Map:
		err = parseMap(w, node)
	case schema.Union:
		err = parseUnion(w, node)
	case schema.Variant:
		err = parseVariant(w, node)
	default:
		if f.Type.IsPrimitive() {
			err = parsePrimitive(w, node)
		} else {
			err = fmt.Errorf("unsupported field type %s", f.Type)
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func parsePrimitive(w *schema.Writer, node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected scalar value, got %s", nodeKind(node))
	}

	f := w.Field()

	switch f.Type {
	case schema.Timestamp:
		// yaml uses custom rfc3339nano format
		tm, err := time.Parse(time.RFC3339Nano, node.Value)
		if err != nil {
			return err
		}
		return w.Write(tm)
	case schema.Time:
		tm, err := schema.TIME_SCALE_SECOND.Parse(node.Value, true)
		if err != nil {
			return err
		}
		return w.Write(tm)
	case schema.Date:
		tm, err := schema.TIME_SCALE_DAY.Parse(node.Value, false)
		if err != nil {
			return err
		}
		return w.Write(tm)
	default:
		parser := parse.NewParser(f.Type, f.Scale, f.Enum)
		parsed, err := parser.ParseValue(node.Value)
		if err != nil {
			return err
		}
		if err := w.Write(parsed); err != nil {
			return err
		}
	}

	return nil
}

func parseBytes(w *schema.Writer, node *yaml.Node) error {
	switch node.Kind {
	case yaml.SequenceNode:
		buf := make([]byte, 0, len(node.Content))
		for _, v := range node.Content {
			if v.Kind != yaml.ScalarNode {
				return fmt.Errorf("expected list of bytes, got %s", nodeKind(v))
			}
			b, err := strconv.ParseUint(v.Value, 10, 8)
			if err != nil {
				return err
			}
			buf = append(buf, uint8(b))
		}
		return w.WriteBytes(buf)
	case yaml.ScalarNode:
		buf, err := hex.DecodeString(node.Value)
		if err != nil {
			return err
		}
		return w.WriteBytes(buf)

	default:
		return fmt.Errorf("expected list or scalar, got %s", nodeKind(node))
	}
}

func parseList(w *schema.Writer, node *yaml.Node) error {
	if node.Kind != yaml.SequenceNode {
		return fmt.Errorf("expected list, got %s", nodeKind(node))
	}

	return w.WriteList(func(lw *schema.ListWriter) error {
		for _, item := range node.Content {
			var err error
			if lw.Schema().NumFields() == 1 {
				err = parseField(lw.Writer, item)
			} else {
				err = parseStruct(lw.Writer, lw.Schema(), item)
			}
			if err != nil {
				return err
			}
			lw.Next()
		}
		return nil
	})
}

func parseMap(w *schema.Writer, node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("expected map, got %s", nodeKind(node))
	}
	f := w.Field()
	numFields := f.Child.NumFields()
	valType := f.ValueType()

	return w.WriteMap(func(mw *schema.MapWriter) error {
		for i := 0; i < len(node.Content); i += 2 {
			keyNode := node.Content[i]
			valNode := node.Content[i+1]
			err := parseField(mw.Writer, keyNode)
			if err != nil {
				return err
			}
			if numFields == 2 {
				err = parseField(mw.Writer, valNode)
			} else {
				err = parseStruct(mw.Writer, valType, valNode)
			}
			if err != nil {
				return err
			}
			mw.Next()
		}
		return nil
	})
}

func parseUnion(w *schema.Writer, node *yaml.Node) error {
	if node.Kind != yaml.MappingNode || len(node.Content) != 4 {
		return fmt.Errorf("union must contain type and value (got %d keys)", len(node.Content)/2)
	}

	yamlMap := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		yamlMap[node.Content[i].Value] = node.Content[i+1]
	}

	ty, ok := yamlMap[UnionTypeKey]
	if !ok {
		return fmt.Errorf("missing union type")
	}
	va, ok := yamlMap[UnionValueKey]
	if !ok {
		return fmt.Errorf("missing union value")
	}

	typ := schema.ParseFieldType(ty.Value)
	if !typ.IsValid() {
		return fmt.Errorf("invalid type %q", ty.Value)
	}
	if !typ.CanUnion() {
		return fmt.Errorf("invalid type %q for union", ty.Value)
	}

	parser := parse.NewParser(typ, 0, nil)
	val, err := parser.ParseValue(va.Value)
	if err != nil {
		return fmt.Errorf("union value %q: %v", va.Value, err)
	}

	u, ok := schema.MakeUnionValue(typ, val)
	if !ok {
		return fmt.Errorf("invalid union value %q", va.Value)
	}

	return w.WriteUnion(u)
}

func parseVariant(w *schema.Writer, node *yaml.Node) error {
	if node.Kind != yaml.MappingNode || len(node.Content) != 2 {
		return fmt.Errorf("variant must contain exactly one case (got %d keys)", len(node.Content)/2)
	}

	// read case
	caseName := node.Content[0].Value
	caseValueNode := node.Content[1]

	// Find matching case
	caseId, caseSchema := w.Field().FindCase(caseName)
	if caseSchema == nil {
		return fmt.Errorf("unknown variant case %q", caseName)
	}

	return w.WriteVariant(caseId, func(vw *schema.VariantWriter) error {
		return parseStruct(vw.Writer, caseSchema, caseValueNode)
	})
}

func nodeKind(n *yaml.Node) string {
	switch n.Kind {
	case yaml.ScalarNode:
		return "scalar"
	case yaml.MappingNode:
		return "mapping"
	case yaml.SequenceNode:
		return "sequence"
	default:
		return fmt.Sprintf("kind=%d", n.Kind)
	}
}
