// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
)

type BinaryCondition struct {
	Left  *Field
	Right *Field
	Mode  FilterMode
}

func NewPredicate(left, right string, mode FilterMode) BinaryCondition {
	return BinaryCondition{
		Left:  &Field{Name: left},
		Right: &Field{Name: right},
		Mode:  mode,
	}
}

func (b *BinaryCondition) Bind(l, r Table) {
	b.Left = l.Fields().Find(b.Left.Name)
	b.Right = r.Fields().Find(b.Right.Name)
}

func (b BinaryCondition) Check() error {
	if !b.Left.IsValid() {
		return fmt.Errorf("pack: invalid left field '%s' in binary condition", b.Left.Name)
	}
	if !b.Right.IsValid() {
		return fmt.Errorf("pack: invalid right field '%s' in binary condition", b.Right.Name)
	}
	if b.Left.Type != b.Right.Type {
		return fmt.Errorf("pack: field types '%s'/'%s' don't match in binary condition",
			b.Left.Type, b.Right.Type)
	}
	switch b.Mode {
	case FilterModeEqual, FilterModeNotEqual, FilterModeGt, FilterModeGte, FilterModeLt, FilterModeLte:
	default:
		return fmt.Errorf("pack: invalid filter mode '%s' for binary condition", b.Mode)
	}
	return nil
}

func (b BinaryCondition) MatchPacksAt(p1 *Package, n1 int, p2 *Package, n2 int) bool {
	v1, err := p1.FieldAt(b.Left.Index, n1)
	if err != nil {
		return false
	}
	v2, err := p2.FieldAt(b.Right.Index, n2)
	if err != nil {
		return false
	}
	switch b.Mode {
	case FilterModeEqual:
		return b.Left.Type.Equal(v1, v2)
	case FilterModeNotEqual:
		return !b.Left.Type.Equal(v1, v2)
	case FilterModeGt:
		return b.Left.Type.Gt(v1, v2)
	case FilterModeGte:
		return b.Left.Type.Gte(v1, v2)
	case FilterModeLt:
		return b.Left.Type.Lt(v1, v2)
	case FilterModeLte:
		return b.Left.Type.Lte(v1, v2)
	default:
		return false
	}
}

func (b BinaryCondition) ComparePacksAt(p1 *Package, n1 int, p2 *Package, n2 int) int {
	v1, err := p1.FieldAt(b.Left.Index, n1)
	if err != nil {
		return -1
	}
	v2, err := p2.FieldAt(b.Right.Index, n2)
	if err != nil {
		return -1
	}
	return b.Left.Type.Compare(v1, v2)
}
