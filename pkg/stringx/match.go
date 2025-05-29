// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

type Bitset = bitset.Bitset

func (p *StringPool) Matcher() types.StringMatcher {
	return p
}

func (p *StringPool) MatchEqual(val []byte, bits, mask *Bitset) {
	if mask != nil {
		for i := range mask.Iterator() {
			ofs, len := ptr2pair(p.ptr[i])
			if !bytes.Equal(p.buf[ofs:ofs+len], val) {
				continue
			}
			bits.Set(i)
		}
	} else {
		var (
			set = bits.Bytes()
			b   byte
			cnt int
		)
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			k := i & 7
			bit := util.Bool2byte(bytes.Equal(p.buf[ofs:ofs+len], val))
			b |= bit << k
			cnt += int(bit)
			if k == 7 {
				set[i>>3] = b
				b = 0
			}
		}
		if l := len(p.ptr); l&7 > 0 {
			set[l>>3] = b
		}
		bits.ResetCount(cnt)
	}
}

func (p *StringPool) MatchNotEqual(val []byte, bits, mask *Bitset) {
	if mask != nil {
		for i := range mask.Iterator() {
			ofs, len := ptr2pair(p.ptr[i])
			if bytes.Equal(p.buf[ofs:ofs+len], val) {
				continue
			}
			bits.Set(i)
		}
	} else {
		var (
			set = bits.Bytes()
			b   byte
			cnt int
		)
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			k := i & 7
			bit := util.Bool2byte(!bytes.Equal(p.buf[ofs:ofs+len], val))
			b |= bit << k
			cnt += int(bit)
			if k == 7 {
				set[i>>3] = b
				b = 0
			}
		}
		if l := len(p.ptr); l&7 > 0 {
			set[l>>3] = b
		}
		bits.ResetCount(cnt)
	}
}

func (p *StringPool) MatchLess(val []byte, bits, mask *Bitset) {
	if mask != nil {
		for i := range mask.Iterator() {
			ofs, len := ptr2pair(p.ptr[i])
			if bytes.Compare(p.buf[ofs:ofs+len], val) >= 0 {
				continue
			}
			bits.Set(i)
		}
	} else {
		var (
			set = bits.Bytes()
			b   byte
			cnt int
		)
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			k := i & 7
			bit := util.Bool2byte(bytes.Compare(p.buf[ofs:ofs+len], val) < 0)
			b |= bit << k
			cnt += int(bit)
			if k == 7 {
				set[i>>3] = b
				b = 0
			}
		}
		if l := len(p.ptr); l&7 > 0 {
			set[l>>3] = b
		}
		bits.ResetCount(cnt)
	}
}

func (p *StringPool) MatchLessEqual(val []byte, bits, mask *Bitset) {
	if mask != nil {
		for i := range mask.Iterator() {
			ofs, len := ptr2pair(p.ptr[i])
			if bytes.Compare(p.buf[ofs:ofs+len], val) > 0 {
				continue
			}
			bits.Set(i)
		}
	} else {
		var (
			set = bits.Bytes()
			b   byte
			cnt int
		)
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			k := i & 7
			bit := util.Bool2byte(bytes.Compare(p.buf[ofs:ofs+len], val) <= 0)
			b |= bit << k
			cnt += int(bit)
			if k == 7 {
				set[i>>3] = b
				b = 0
			}
		}
		if l := len(p.ptr); l&7 > 0 {
			set[l>>3] = b
		}
		bits.ResetCount(cnt)
	}
}

func (p *StringPool) MatchGreater(val []byte, bits, mask *Bitset) {
	if mask != nil {
		for i := range mask.Iterator() {
			ofs, len := ptr2pair(p.ptr[i])
			if bytes.Compare(p.buf[ofs:ofs+len], val) <= 0 {
				continue
			}
			bits.Set(i)
		}
	} else {
		var (
			set = bits.Bytes()
			b   byte
			cnt int
		)
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			k := i & 7
			bit := util.Bool2byte(bytes.Compare(p.buf[ofs:ofs+len], val) > 0)
			b |= bit << k
			cnt += int(bit)
			if k == 7 {
				set[i>>3] = b
				b = 0
			}
		}
		if l := len(p.ptr); l&7 > 0 {
			set[l>>3] = b
		}
		bits.ResetCount(cnt)
	}
}

func (p *StringPool) MatchGreaterEqual(val []byte, bits, mask *Bitset) {
	if mask != nil {
		for i := range mask.Iterator() {
			ofs, len := ptr2pair(p.ptr[i])
			if bytes.Compare(p.buf[ofs:ofs+len], val) < 0 {
				continue
			}
			bits.Set(i)
		}
	} else {
		var (
			set = bits.Bytes()
			b   byte
			cnt int
		)
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			k := i & 7
			bit := util.Bool2byte(bytes.Compare(p.buf[ofs:ofs+len], val) >= 0)
			b |= bit << k
			cnt += int(bit)
			if k == 7 {
				set[i>>3] = b
				b = 0
			}
		}
		if l := len(p.ptr); l&7 > 0 {
			set[l>>3] = b
		}
		bits.ResetCount(cnt)
	}
}

func (p *StringPool) MatchBetween(x, y []byte, bits, mask *Bitset) {
	if mask != nil {
		for i := range mask.Iterator() {
			ofs, len := ptr2pair(p.ptr[i])
			val := p.buf[ofs : ofs+len]
			if bytes.Compare(val, x) < 0 || bytes.Compare(val, y) > 0 {
				continue
			}
			bits.Set(i)
		}
	} else {
		var (
			set = bits.Bytes()
			b   byte
			cnt int
		)
		for i, ptr := range p.ptr {
			ofs, len := ptr2pair(ptr)
			k := i & 7
			val := p.buf[ofs : ofs+len]
			bit := util.Bool2byte(bytes.Compare(val, x) >= 0 && bytes.Compare(val, y) <= 0)
			b |= bit << k
			cnt += int(bit)
			if k == 7 {
				set[i>>3] = b
				b = 0
			}
		}
		if l := len(p.ptr); l&7 > 0 {
			set[l>>3] = b
		}
		bits.ResetCount(cnt)
	}
}
