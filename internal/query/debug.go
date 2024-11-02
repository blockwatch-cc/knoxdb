// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"blockwatch.cc/knoxdb/internal/types"
)

func (n FilterTreeNode) Dump() string {
	buf := bytes.NewBuffer(nil)
	n.dump(0, buf)
	return string(buf.Bytes())
}

func (n FilterTreeNode) dump(level int, w io.Writer) {
	if n.IsLeaf() {
		fmt.Fprint(w, n.Filter.String())
	}
	kind := " AND "
	if n.OrKind {
		kind = " OR "
	}
	if len(n.Children) > 0 {
		fmt.Fprint(w, " ( ")
		defer fmt.Fprint(w, " ) ")
	}
	for i, v := range n.Children {
		if i > 0 {
			fmt.Fprint(w, kind)
		}
		v.dump(level+1, w)
	}
}

func (q QueryPlan) Dump() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintf(buf, "Q> %s => SELECT ( %s ) WHERE", q.Tag, strings.Join(q.ResultSchema.AllFieldNames(), ", "))
	q.Filters.dump(0, buf)
	if q.Order != types.OrderAsc {
		fmt.Fprintf(buf, "ORDER BY ID %s ", strings.ToUpper(q.Order.String()))
	}
	if q.Limit > 0 {
		fmt.Fprintf(buf, "LIMIT %d", q.Limit)
	}
	for i, n := range []string{"NOCACHE", "NOINDEX", "DEBUG", "STATS"} {
		if q.Flags&(1<<i) > 0 {
			fmt.Fprintf(buf, " %s", n)
		}
	}
	return string(buf.Bytes())
}

// func (j Join) Dump() string {
// 	buf := bytes.NewBuffer(nil)
// 	fmt.Fprintln(buf, "Join:", j.Type.String(), "=>")
// 	fmt.Fprintln(buf, "  Predicate:", j.Predicate.Left.Alias, j.Predicate.Mode.String(), j.Predicate.Right.Alias)
// 	fmt.Fprintln(buf, "  Left:", j.Left.Table.Name())
// 	fmt.Fprintln(buf, "  Where:")
// 	j.Left.Where.dump(0, buf)
// 	fmt.Fprintln(buf, "  Fields:", strings.Join(j.Left.Fields, ","))
// 	fmt.Fprintln(buf, "  AS:", strings.Join(j.Left.FieldsAs, ","))
// 	fmt.Fprintln(buf, "  Limit:", j.Left.Limit)
// 	fmt.Fprintln(buf, "  Right:", j.Right.Table.Name())
// 	fmt.Fprintln(buf, "  Where:")
// 	j.Right.Where.dump(0, buf)
// 	fmt.Fprintln(buf, "  Fields:", strings.Join(j.Right.Fields, ","))
// 	fmt.Fprintln(buf, "  AS:", strings.Join(j.Right.FieldsAs, ","))
// 	fmt.Fprintln(buf, "  Limit:", j.Right.Limit)
// 	return string(buf.Bytes())
// }

// func (r Result) Dump() string {
// 	buf := bytes.NewBuffer(nil)
// 	fmt.Fprintf(buf, "Result ------------------------------------ \n")
// 	fmt.Fprintf(buf, "Rows:       %d\n", r.Rows())
// 	fmt.Fprintf(buf, "Cols:       %d\n", len(r.fields))
// 	fmt.Fprintf(buf, "%-2s  %-15s  %-15s  %-10s  %-4s  %s\n", "No", "Name", "Alias", "Type", "Scale", "Flags")
// 	for _, v := range r.fields {
// 		fmt.Fprintf(buf, "%02d  %-15s  %-15s  %-10s  %2d    %s\n",
// 			v.Index, v.Name, v.Alias, v.Type, v.Scale, v.Flags)
// 	}
// 	return buf.String()
// }
