// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"bytes"
	"fmt"
	"sort"
)

const packStatsVersion byte = 1

type PackStats struct {
	Key        uint32       // pack key
	SchemaId   uint64       // storage schema
	NValues    int          // rows in pack
	Blocks     []BlockStats // list of block metadata
	StoredSize int          // block size on disk
	Dirty      bool
}

func (p PackStats) IsValid() bool {
	return p.NValues > 0
}

func (p PackStats) KeyBytes() []byte {
	var b [4]byte
	BE.PutUint32(b[:], p.Key)
	return b[:]
}

func (m PackStats) HeapSize() int {
	sz := szPackStats + len(m.Blocks)*(szBlockStats+16)
	for i := range m.Blocks {
		sz += m.Blocks[i].HeapSize()
	}
	return sz
}

func (m PackStats) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := m.Encode(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *PackStats) UnmarshalBinary(data []byte) error {
	return m.Decode(bytes.NewBuffer(data))
}

func (m PackStats) Encode(buf *bytes.Buffer) error {
	buf.WriteByte(packStatsVersion)
	var b [8]byte
	BE.PutUint32(b[:], uint32(m.Key))
	buf.Write(b[:4])
	BE.PutUint64(b[:], m.SchemaId)
	buf.Write(b[:])
	BE.PutUint32(b[:], uint32(m.NValues))
	buf.Write(b[:4])
	BE.PutUint32(b[:], uint32(m.StoredSize))
	buf.Write(b[:4])
	BE.PutUint32(b[:], uint32(len(m.Blocks)))
	buf.Write(b[:4])
	for _, b := range m.Blocks {
		if err := b.Encode(buf); err != nil {
			return err
		}
	}
	return nil
}

func (m *PackStats) Decode(buf *bytes.Buffer) error {
	if buf.Len() < 25 {
		return fmt.Errorf("knox: short pack metadata buffer len=%d", buf.Len())
	}

	// read and check version byte
	ver, _ := buf.ReadByte()
	if ver > packStatsVersion {
		return fmt.Errorf("knox: unexpected pack metadata version %d", ver)
	}

	// read pack metadata header
	m.Key = BE.Uint32(buf.Next(4))
	m.SchemaId = BE.Uint64(buf.Next(8))
	m.NValues = int(BE.Uint32(buf.Next(4)))
	m.StoredSize = int(BE.Uint32(buf.Next(4)))

	// read block metadata
	m.Blocks = make([]BlockStats, int(BE.Uint32(buf.Next(4))))
	for i := range m.Blocks {
		if err := m.Blocks[i].Decode(buf, ver); err != nil {
			return err
		}
	}
	return nil
}

type PackStatsList []*PackStats

func (l PackStatsList) Len() int           { return len(l) }
func (l PackStatsList) Less(i, j int) bool { return l[i].Key < l[j].Key }
func (l PackStatsList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func (l *PackStatsList) Add(info *PackStats) (*PackStats, int, bool) {
	i := sort.Search(l.Len(), func(i int) bool {
		return (*l)[i].Key >= info.Key
	})
	if i < len(*l) && (*l)[i].Key == info.Key {
		oldhead := (*l)[i]
		(*l)[i] = info
		return oldhead, i, false
	}
	*l = append(*l, nil)
	copy((*l)[i+1:], (*l)[i:])
	(*l)[i] = info
	return nil, i, true
}

func (l *PackStatsList) Remove(head *PackStats) (*PackStats, int) {
	return l.RemoveKey(head.Key)
}

func (l *PackStatsList) RemoveKey(key uint32) (*PackStats, int) {
	i := sort.Search(l.Len(), func(i int) bool {
		return (*l)[i].Key >= key
	})
	if i < len(*l) && (*l)[i].Key == key {
		oldhead := (*l)[i]
		*l = append((*l)[:i], (*l)[i+1:]...)
		return oldhead, i
	}
	return nil, -1
}

func (l PackStatsList) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	for _, v := range l {
		if err := v.Encode(buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (l *PackStatsList) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("knox: short metadata len=%d", len(data))
	}
	buf := bytes.NewBuffer(data)
	n := int(BE.Uint32(buf.Next(4)))
	*l = make(PackStatsList, n)
	for i := range *l {
		if err := (*l)[i].Decode(buf); err != nil {
			return err
		}
	}
	return nil
}
