// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

import (
	"errors"
	"fmt"

	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	ErrNoName    = errors.New("missing field name")
	ErrNoMode    = errors.New("invalid filter mode")
	ErrNoMatcher = errors.New("missing matcher")
	ErrNoValue   = errors.New("missing value")
)

type Filter struct {
	Name    string     // schema field name
	Type    BlockType  // block type (we need for opimizing filter trees)
	Mode    FilterMode // eq|ne|gt|gte|lt|lte|rg|in|nin|re
	Index   int        // field index (use with pack.Package.Block() and schema.View.Get())
	Id      uint16     // field unique id (used as storage key)
	Matcher Matcher    // encapsulated match data and function
	Value   any        // direct val for eq|ne|gt|ge|lt|le, [2]any for rg, slice for in|nin, string re
}

func NewFilter(f *schema.Field, idx int, mode FilterMode, val any) *Filter {
	m := newFactory(f.Type().BlockType()).New(mode)
	m.WithValue(val)
	return &Filter{
		Name:    f.Name(),
		Type:    f.Type().BlockType(),
		Mode:    mode,
		Index:   idx,
		Id:      f.Id(),
		Value:   val,
		Matcher: m,
	}
}

func (f *Filter) Weight() int {
	return f.Matcher.Weight()
}

func (f *Filter) Validate() error {
	if f.Name == "" {
		return ErrNoName
	}
	if !f.Mode.IsValid() {
		return ErrNoMode
	}
	switch f.Mode {
	case FilterModeTrue, FilterModeFalse:
		// empty matcher or value ok
	default:
		if f.Matcher == nil {
			return ErrNoMatcher
		}
		if f.Value == nil {
			return ErrNoValue
		}
	}
	return nil
}

func (f *Filter) String() string {
	return fmt.Sprintf("%s[id=%d,n=%d] %s %s",
		f.Name,
		f.Id,
		f.Index,
		f.Mode.Symbol(),
		util.ToString(f.Value),
	)
}

func (f *Filter) AsFalse() *Filter {
	return &Filter{
		Name:    f.Name,
		Type:    f.Type,
		Mode:    FilterModeFalse,
		Index:   f.Index,
		Id:      f.Id,
		Matcher: &noopMatcher{},
		Value:   nil,
	}
}

func (f *Filter) AsTrue() *Filter {
	return &Filter{
		Name:    f.Name,
		Type:    f.Type,
		Mode:    FilterModeTrue,
		Index:   f.Index,
		Id:      f.Id,
		Matcher: &noopMatcher{},
		Value:   nil,
	}
}

func (f *Filter) As(mode FilterMode, val any) *Filter {
	m := newFactory(f.Type).New(mode)
	m.WithValue(val)
	return &Filter{
		Name:    f.Name,
		Type:    f.Type,
		Mode:    mode,
		Index:   f.Index,
		Id:      f.Id,
		Matcher: m,
		Value:   val,
	}
}

func (f *Filter) AsSet(set *xroar.Bitmap) *Filter {
	m := newFactory(f.Type).New(FilterModeIn)
	m.WithSet(set)
	return &Filter{
		Name:    f.Name,
		Type:    f.Type,
		Mode:    FilterModeIn,
		Index:   f.Index,
		Id:      f.Id,
		Matcher: m,
		Value:   m.Value(), // FIXME: optimizer expects []T which is expensive
	}
}
