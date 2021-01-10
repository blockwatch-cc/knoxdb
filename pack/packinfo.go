// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

type PackInfo struct {
	Key     uint32
	NValues int
	Blocks  BlockInfoList
	Size    int

	// not stored
	dirty bool
}

func (p PackInfo) KeyBytes() []byte {
	var b [4]byte
	bigEndian.PutUint32(b[:], p.Key)
	return b[:]
}

func (p *Package) Info() PackInfo {
	h := PackInfo{
		Key:     p.key,
		NValues: p.nValues,
		Blocks:  make(BlockInfoList, p.nFields),
		Size:    p.size,
		dirty:   true,
	}
	for i, v := range p.blocks {
		h.Blocks[i] = NewBlockInfo(v, p.fields[i])
	}
	return h
}

func (h *PackInfo) UpdateStats(pkg *Package) error {
	if h.Key != pkg.key {
		return fmt.Errorf("pack: info key mismatch %x/%d ", h.Key, pkg.key)
	}
	for i := range h.Blocks {
		if have, want := h.Blocks[i].Type, pkg.blocks[i].Type(); have != want {
			return fmt.Errorf("pack: block type mismatch in pack %x/%d: %s != %s ",
				h.Key, i, have, want)
		}
		if !h.Blocks[i].IsDirty() {
			continue
		}
		// EXPENSIVE: collects full min/max statistics
		h.Blocks[i].MinValue, h.Blocks[i].MaxValue = pkg.blocks[i].MinMax()
		h.Blocks[i].dirty = false

		// signal that this pack info must be saved
		h.dirty = true
	}
	return nil
}

func (h PackInfo) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := h.Encode(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *PackInfo) UnmarshalBinary(data []byte) error {
	return h.Decode(bytes.NewBuffer(data))
}

func (h PackInfo) Encode(buf *bytes.Buffer) error {
	var b [4]byte
	bigEndian.PutUint32(b[0:], uint32(h.Key))
	buf.Write(b[:])
	bigEndian.PutUint32(b[0:], uint32(h.NValues))
	buf.Write(b[:])
	bigEndian.PutUint32(b[0:], uint32(h.Size))
	buf.Write(b[:])
	return h.Blocks.Encode(buf)
}

func (h *PackInfo) Decode(buf *bytes.Buffer) error {
	h.Key = bigEndian.Uint32(buf.Next(4))
	h.NValues = int(bigEndian.Uint32(buf.Next(4)))
	h.Size = int(bigEndian.Uint32(buf.Next(4)))
	return h.Blocks.Decode(buf)
}

type PackInfoList []PackInfo

func (l PackInfoList) Len() int           { return len(l) }
func (l PackInfoList) Less(i, j int) bool { return l[i].Key < l[j].Key }
func (l PackInfoList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func (l *PackInfoList) Add(info PackInfo) (PackInfo, int, bool) {
	i := sort.Search(l.Len(), func(i int) bool {
		return (*l)[i].Key >= info.Key
	})
	if i < len(*l) && (*l)[i].Key == info.Key {
		oldhead := (*l)[i]
		(*l)[i] = info
		return oldhead, i, false
	}
	*l = append(*l, PackInfo{})
	copy((*l)[i+1:], (*l)[i:])
	(*l)[i] = info
	return PackInfo{}, i, true
}

func (l *PackInfoList) Remove(head PackInfo) (PackInfo, int) {
	return l.RemoveKey(head.Key)
}

func (l *PackInfoList) RemoveKey(key uint32) (PackInfo, int) {
	i := sort.Search(l.Len(), func(i int) bool {
		return (*l)[i].Key >= key
	})
	if i < len(*l) && (*l)[i].Key == key {
		oldhead := (*l)[i]
		*l = append((*l)[:i], (*l)[i+1:]...)
		return oldhead, i
	}
	return PackInfo{}, -1
}

func (h PackInfoList) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte(packageStorageFormatVersionV1)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(h)))
	buf.Write(b[:])
	for _, v := range h {
		if err := v.Encode(buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (h *PackInfoList) UnmarshalBinary(data []byte) error {
	if len(data) < 5 {
		return fmt.Errorf("pack: short package info list header, length %d", len(data))
	}
	buf := bytes.NewBuffer(data)

	b, _ := buf.ReadByte()
	if b != currentStorageFormat {
		return fmt.Errorf("pack: invalid package info list header version %d", b)
	}

	l := int(binary.BigEndian.Uint32(buf.Next(4)))

	*h = make(PackInfoList, l)

	for i := range *h {
		if err := (*h)[i].Decode(buf); err != nil {
			return err
		}
	}
	return nil
}