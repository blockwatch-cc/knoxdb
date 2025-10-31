// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
)

type Manifest struct {
	CreatedAt time.Time
	Name      string
	Label     string
}

func NewManifest(name, label string) *Manifest {
	return &Manifest{
		CreatedAt: time.Now().UTC(),
		Name:      name,
		Label:     label,
	}
}

func NewManifestFromOpts(opts Options) *Manifest {
	name := strings.TrimSuffix(filepath.Base(opts.Path), filepath.Ext(opts.Path))
	return NewManifest(name, "")
}

func (m *Manifest) IsValid() bool {
	return len(m.Name) > 0
}

func (m *Manifest) Validate(v *Manifest) error {
	if v == nil {
		return nil
	}
	if v.Name != "*" && v.Name != m.Name {
		return fmt.Errorf("manifest: invalid name, want=%s have=%s", v.Name, m.Name)
	}
	if v.Label != "*" && v.Label != m.Label {
		return fmt.Errorf("manifest: invalid label, want=%s have=%s", v.Label, m.Label)
	}
	return nil
}

func (m *Manifest) Bytes() []byte {
	buf, _ := m.MarshalBinary()
	return buf
}

func (m *Manifest) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, 128)
	buf = num.AppendUvarint(buf, uint64(m.CreatedAt.Unix()))
	buf = num.AppendUvarint(buf, uint64(len(m.Name)))
	buf = append(buf, []byte(m.Name)...)
	buf = num.AppendUvarint(buf, uint64(len(m.Label)))
	buf = append(buf, []byte(m.Label)...)
	return buf, nil
}

func (m *Manifest) UnmarshalBinary(buf []byte) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = io.ErrShortBuffer
		}
	}()
	v, n := num.Uvarint(buf)
	buf = buf[n:]
	m.CreatedAt = time.Unix(int64(v), 0)

	v, n = num.Uvarint(buf)
	buf = buf[n:]
	m.Name = string(buf[:v])
	buf = buf[v:]

	v, n = num.Uvarint(buf)
	buf = buf[n:]
	m.Label = string(buf[:v])

	return nil
}
