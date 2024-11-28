// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"encoding/hex"
	"fmt"
	"strings"
)

func (a *DictByteArray) Dump() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Dict log2=%d n=%d len(dict)=%d len(offs)=%d len(size)=%d len(ptr)=%d\n",
		a.log2, a.n, len(a.dict), len(a.offs), len(a.size), len(a.ptr)))
	for i := range a.offs {
		if i > 0 && i%5 == 0 {
			b.WriteString("\n")
		}
		b.WriteString(fmt.Sprintf("%4d: %5d/%-5d  ", i, a.offs[i], a.size[i]))
	}
	b.WriteString("\nPTR\n")
	b.WriteString(hex.Dump(a.ptr))
	b.WriteString("\nDICT\n")
	b.WriteString(hex.Dump(a.dict))
	b.WriteString("\n")
	return b.String()
}

func (a *CompactByteArray) Dump() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Compact n=%d len(data)=%d len(offs)=%d len(size)=%d\n",
		a.Len(), len(a.buf), len(a.offs), len(a.size)))
	for i := range a.offs {
		if i > 0 && i%5 == 0 {
			b.WriteString("\n")
		}
		b.WriteString(fmt.Sprintf("%4d: %5d/%-5d %s ", i, a.offs[i], a.size[i], hex.EncodeToString(a.Elem(i))))
	}
	b.WriteString("\nDATA\n")
	b.WriteString(hex.Dump(a.buf))
	b.WriteString("\n")
	return b.String()
}

func (a *FixedByteArray) Dump() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Fixed n=%d sz=%d len(data)=%d\n", a.n, a.sz, len(a.buf)))
	for i := 0; i < a.n; i++ {
		if i > 0 && i%5 == 0 {
			b.WriteString("\n")
		}
		b.WriteString(fmt.Sprintf("%4d: %[2]*[3]s  ", i, a.sz, hex.EncodeToString(a.Elem(i))))
	}
	b.WriteString("\n")
	return b.String()
}

func (a *NativeByteArray) Dump() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Native n=%d\n", len(a.bufs)))
	for i, v := range a.bufs {
		if i > 0 && i%5 == 0 {
			b.WriteString("\n")
		}
		b.WriteString(fmt.Sprintf("%4d: %32s  ", i, hex.EncodeToString(v)))
	}
	b.WriteString("\n")
	return b.String()
}
