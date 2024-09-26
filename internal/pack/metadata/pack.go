// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"bytes"
	"fmt"
	"sort"
)

const packMetadataVersion byte = 1

type PackMetadata struct {
	Key        uint32          // pack key
	SchemaId   uint64          // storage schema
	NValues    int             // rows in pack
	Blocks     []BlockMetadata // list of block metadata
	StoredSize int             // block size on disk
	Dirty      bool
}

func (p PackMetadata) IsValid() bool {
	return p.NValues > 0
}

func (p PackMetadata) KeyBytes() []byte {
	var b [4]byte
	BE.PutUint32(b[:], p.Key)
	return b[:]
}

func (m PackMetadata) HeapSize() int {
	sz := szPackMetadata + len(m.Blocks)*(szBlockMetadata+16)
	for i := range m.Blocks {
		sz += m.Blocks[i].HeapSize()
	}
	return sz
}

func (m PackMetadata) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := m.Encode(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *PackMetadata) UnmarshalBinary(data []byte) error {
	return m.Decode(bytes.NewBuffer(data))
}

func (m PackMetadata) Encode(buf *bytes.Buffer) error {
	buf.WriteByte(packMetadataVersion)
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

func (m *PackMetadata) Decode(buf *bytes.Buffer) error {
	if buf.Len() < 25 {
		return fmt.Errorf("knox: short pack metadata buffer len=%d", buf.Len())
	}

	// read and check version byte
	ver, _ := buf.ReadByte()
	if ver > packMetadataVersion {
		return fmt.Errorf("knox: unexpected pack metadata version %d", ver)
	}

	// read pack metadata header
	m.Key = BE.Uint32(buf.Next(4))
	m.SchemaId = BE.Uint64(buf.Next(8))
	m.NValues = int(BE.Uint32(buf.Next(4)))
	m.StoredSize = int(BE.Uint32(buf.Next(4)))

	// read block metadata
	m.Blocks = make([]BlockMetadata, int(BE.Uint32(buf.Next(4))))
	for i := range m.Blocks {
		if err := m.Blocks[i].Decode(buf, ver); err != nil {
			return err
		}
	}
	return nil
}

type PackMetadataList []*PackMetadata

func (l PackMetadataList) Len() int           { return len(l) }
func (l PackMetadataList) Less(i, j int) bool { return l[i].Key < l[j].Key }
func (l PackMetadataList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func (l *PackMetadataList) Add(info *PackMetadata) (*PackMetadata, int, bool) {
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

func (l *PackMetadataList) Remove(head *PackMetadata) (*PackMetadata, int) {
	return l.RemoveKey(head.Key)
}

func (l *PackMetadataList) RemoveKey(key uint32) (*PackMetadata, int) {
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

func (l PackMetadataList) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	for _, v := range l {
		if err := v.Encode(buf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (l *PackMetadataList) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("knox: short metadata len=%d", len(data))
	}
	buf := bytes.NewBuffer(data)
	n := int(BE.Uint32(buf.Next(4)))
	*l = make(PackMetadataList, n)
	for i := range *l {
		if err := (*l)[i].Decode(buf); err != nil {
			return err
		}
	}
	return nil
}
