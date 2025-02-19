// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"bytes"
	"context"
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var _ Store = (*StoreImpl)(nil)

type StoreImpl struct {
	db    Database
	store engine.StoreEngine
}

func (s StoreImpl) DB() Database {
	return s.db
}

func (s StoreImpl) Schema() *schema.Schema {
	return s.store.Schema()
}

func (s StoreImpl) Metrics() StoreMetrics {
	return s.store.Metrics()
}

func (s *StoreImpl) Get(ctx context.Context, key []byte) ([]byte, error) {
	return s.store.Get(ctx, key)
}

func (s *StoreImpl) Put(ctx context.Context, key, val []byte) error {
	return s.store.Put(ctx, key, val)
}

func (s *StoreImpl) Del(ctx context.Context, key []byte) error {
	return s.store.Del(ctx, key)
}

func (s *StoreImpl) Range(ctx context.Context, prefix []byte, fn func(ctx context.Context, k, v []byte) error) error {
	return s.store.Range(ctx, prefix, fn)
}

func (s *StoreImpl) Scan(ctx context.Context, from, to []byte, fn func(ctx context.Context, k, v []byte) error) error {
	return s.store.Scan(ctx, from, to, fn)
}

// GenericStore[T] implements Store interface for Go struct types an uint64 keys
type GenericStore[T any] struct {
	db    Database
	enc   *schema.GenericEncoder[T]
	dec   *schema.GenericDecoder[T]
	buf   *bytes.Buffer
	store engine.StoreEngine
}

func UseGenericStore[T any](name string, db Database) (*GenericStore[T], error) {
	var t T
	s, err := schema.SchemaOf(t)
	if err != nil {
		return nil, err
	}
	store, err := db.UseStore(name)
	if err != nil {
		return nil, err
	}
	// check schema matches
	if store.Schema().Hash() != s.Hash() {
		return nil, schema.ErrSchemaMismatch
	}
	return &GenericStore[T]{
		db:    db,
		enc:   schema.NewGenericEncoder[T](), // TODO: link enums
		dec:   schema.NewGenericDecoder[T](), // TODO: link enums
		store: store.(*StoreImpl).store,
	}, nil
}

func (s *GenericStore[T]) DB() Database {
	return s.db
}

func (s *GenericStore[T]) Schema() *schema.Schema {
	return s.enc.Schema()
}

func (s *GenericStore[T]) Metrics() StoreMetrics {
	return s.store.Metrics()
}

func (s *GenericStore[T]) Get(ctx context.Context, key uint64) (*T, error) {
	buf, err := s.store.Get(ctx, Key64Bytes(key))
	if err != nil {
		return nil, err
	}
	val := new(T)
	return s.dec.Decode(buf, val)
}

func (s *GenericStore[T]) Put(ctx context.Context, key uint64, val *T) error {
	if val == nil {
		return schema.ErrNilValue
	}
	if s.buf == nil {
		s.buf = s.enc.NewBuffer(1)
	}
	s.buf.Reset()
	buf, err := s.enc.EncodePtr(val, s.buf)
	if err != nil {
		return err
	}
	return s.store.Put(ctx, Key64Bytes(key), buf)
}

func (s *GenericStore[T]) Del(ctx context.Context, key uint64) error {
	return s.store.Del(ctx, Key64Bytes(key))
}

func (s *GenericStore[T]) Range(ctx context.Context, prefix []byte, fn func(ctx context.Context, k uint64, v *T) error) error {
	return s.store.Range(ctx, prefix, func(ctx context.Context, k, v []byte) error {
		val := new(T)
		s.dec.Decode(v, val)
		return fn(ctx, Key64(k), val)
	})
}

func (s *GenericStore[T]) Scan(ctx context.Context, from, to []byte, fn func(ctx context.Context, k uint64, v *T) error) error {
	return s.store.Scan(ctx, from, to, func(ctx context.Context, k, v []byte) error {
		val := new(T)
		s.dec.Decode(v, val)
		return fn(ctx, Key64(k), val)
	})
}

func Key64Bytes(u64 uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], u64)
	return key[:]
}

func Key64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
