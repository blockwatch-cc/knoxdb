// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
    "fmt"
    "reflect"
    "strings"

    "blockwatch.cc/knoxdb/util"
)

// condition that is not bound to a table field yet
type UnboundCondition struct {
    Name     string
    Mode     FilterMode  // eq|ne|gt|gte|lt|lte|in|nin|re
    Raw      string      // string value when parsed from a query string
    Value    interface{} // typed value
    From     interface{} // typed value for range queries
    To       interface{} // typed value for range queries
    OrKind   bool
    Children []UnboundCondition
}

func (u *UnboundCondition) Clear() {
    u.Name = ""
    u.Mode = 0
    u.Raw = ""
    u.Value = nil
    u.From = nil
    u.To = nil
    u.OrKind = false
    u.Children = nil
}

func (u UnboundCondition) Bind(table *Table) ConditionTreeNode {
    // bind single condition leaf node
    if u.Name != "" {
        return ConditionTreeNode{
            Cond: &Condition{
                Field: table.Fields().Find(u.Name),
                Mode:  u.Mode,
                Raw:   u.Raw,
                Value: u.Value,
                From:  u.From,
                To:    u.To,
            },
        }
    }

    // bind children
    node := ConditionTreeNode{
        OrKind:   u.OrKind,
        Children: make([]ConditionTreeNode, 0),
    }
    for _, v := range u.Children {
        node.Children = append(node.Children, v.Bind(table))
    }
    return node
}

func (u UnboundCondition) Empty() bool {
    return len(u.Children) == 0 && !u.Mode.IsValid()
}

func (u UnboundCondition) Leaf() bool {
    return u.Name != ""
}

func (u *UnboundCondition) And(col string, mode FilterMode, value interface{}) {
    u.Add(UnboundCondition{
        Name:   col,
        Mode:   mode,
        Value:  value,
        OrKind: COND_AND,
    })
}

func (u *UnboundCondition) AndRange(col string, from, to interface{}) {
    u.Add(UnboundCondition{
        Name:   col,
        Mode:   FilterModeRange,
        From:   from,
        To:     to,
        OrKind: COND_AND,
    })
}

func (u *UnboundCondition) Or(col string, mode FilterMode, value interface{}) {
    u.Add(UnboundCondition{
        Name:   col,
        Mode:   mode,
        Value:  value,
        OrKind: COND_OR,
    })
}

func (u *UnboundCondition) OrRange(col string, from, to interface{}) {
    u.Add(UnboundCondition{
        Name:   col,
        Mode:   FilterModeRange,
        From:   from,
        To:     to,
        OrKind: COND_OR,
    })
}

func (u *UnboundCondition) Add(c UnboundCondition) {
    if u.Leaf() {
        clone := UnboundCondition{
            Name:     u.Name,
            Mode:     u.Mode,
            Raw:      u.Raw,
            Value:    u.Value,
            From:     u.From,
            To:       u.To,
            OrKind:   u.OrKind,
            Children: u.Children,
        }
        u.Children = []UnboundCondition{clone}
    }

    // append new condition to this element
    if u.OrKind == c.OrKind && !c.Leaf() {
        u.Children = append(u.Children, c.Children...)
    } else {
        u.Children = append(u.Children, c)
    }
}

func And(conds ...UnboundCondition) UnboundCondition {
    return UnboundCondition{
        Mode:     FilterModeInvalid,
        OrKind:   COND_AND,
        Children: conds,
    }
}

func Or(conds ...UnboundCondition) UnboundCondition {
    return UnboundCondition{
        Mode:     FilterModeInvalid,
        OrKind:   COND_OR,
        Children: conds,
    }
}

func Equal(col string, val interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeEqual, Value: val}
}

func NotEqual(col string, val interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeNotEqual, Value: val}
}

func In(col string, value interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeIn, Value: value}
}

func NotIn(col string, value interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeNotIn, Value: value}
}

func Lt(col string, value interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeLt, Value: value}
}

func Lte(col string, value interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeLte, Value: value}
}

func Gt(col string, value interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeGt, Value: value}
}

func Gte(col string, value interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeGte, Value: value}
}

func Regexp(col string, value interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeRegexp, Value: value}
}

func Range(col string, from, to interface{}) UnboundCondition {
    return UnboundCondition{Name: col, Mode: FilterModeRange, From: from, To: to}
}

func (u UnboundCondition) String() string {
    switch u.Mode {
    case FilterModeRange:
        return fmt.Sprintf("%s %s [%s, %s]",
            u.Name,
            u.Mode.Op(),
            util.ToString(u.From),
            util.ToString(u.To),
        )
    case FilterModeIn, FilterModeNotIn:
        size := reflect.ValueOf(u.Value).Len()
        if size > 16 {
            return fmt.Sprintf("%s %s [%d values]", u.Name, u.Mode.Op(), size)
        } else {
            return fmt.Sprintf("%s %s [%#v]", u.Name, u.Mode.Op(), u.Value)
        }
    default:
        s := fmt.Sprintf("%s %s %s", u.Name, u.Mode.Op(), util.ToString(u.Value))
        if len(u.Raw) > 0 {
            s += " [" + u.Raw + "]"
        }
        return s
    }
}

// parse conditions from query string
// col_name.{gt|gte|lt|lte|ne|in|nin|rg|re}=value
func ParseCondition(key, val string, fields FieldList) (UnboundCondition, error) {
    var (
        c    UnboundCondition
        f, m string
        err  error
    )
    if ff := strings.Split(key, "."); len(ff) == 2 {
        f, m = ff[0], ff[1]
    } else {
        f = ff[0]
        m = "eq"
    }
    field := fields.Find(f)
    if !field.IsValid() {
        return c, fmt.Errorf("unknown column '%s'", f)
    }
    c.Name = field.Name
    c.Mode = ParseFilterMode(m)
    if !c.Mode.IsValid() {
        return c, fmt.Errorf("invalid filter mode '%s'", m)
    }
    c.Raw = val
    switch c.Mode {
    case FilterModeRange:
        vv := strings.Split(val, ",")
        if len(vv) != 2 {
            return c, fmt.Errorf("range conditions require exactly two arguments")
        }
        c.From, err = field.Type.ParseAs(vv[0], field)
        if err != nil {
            return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
        }
        c.To, err = field.Type.ParseAs(vv[1], field)
        if err != nil {
            return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
        }
    case FilterModeIn, FilterModeNotIn:
        c.Value, err = field.Type.ParseSliceAs(val, field)
        if err != nil {
            return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
        }
    default:
        c.Value, err = field.Type.ParseAs(val, field)
        if err != nil {
            return c, fmt.Errorf("error parsing condition value '%s': %v", val, err)
        }
    }
    return c, nil
}
