// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// SchemaRegistry defines an interface for schema registries
// that can be implemented by any host application. Based on
// use case and access patterns users must decide whether to
// implement one registry per application or dedicated registries
// per schema family. Registries may load schemas on demand.
type SchemaRegistry interface {
	// Searches a schema by name and version returning true when found.
	LookupSchemaName(ctx context.Context, name string, ver uint32) (*Schema, bool)

	// Searches a schemy by hash and returns true when found.
	LookupSchemaHash(ctx context.Context, hash uint64) (*Schema, bool)
}

// SchemaResolver defines an interface for batch schema resolvers
// that can be implemented by any host application.
type SchemaResolver interface {
	// ResolveBatch attempts to read schema hash and version from
	// a buffer header and resolve the schema. Should return errors
	// on invalid buffers or when no schema was found.
	ResolveBatch(ctx context.Context, buf []byte) (*Batch, error)

	// ResolveSchema resolves schema from hash and version and
	// returns an error when not found or on version mismatch.
	ResolveSchema(ctx context.Context, buf []byte) (*Schema, error)
}

// Registry implements a basic schema registry that keeps all
// schemas in memory only. It is safe to call concurrently and
// may be used as global application registry, however, due to
// lack of persistence all schemas must be registered at least
// once on start.
type Registry struct {
	mu     sync.RWMutex
	byName map[string]*[]uint64 // name -> version -> hash
	byHash map[uint64]*Schema   // hash -> schema
}

func NewRegistry() *Registry {
	return &Registry{
		byName: make(map[string]*[]uint64),
		byHash: make(map[uint64]*Schema),
	}
}

func (r *Registry) LookupSchemaName(_ context.Context, name string, ver uint32) (*Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	vmap, ok := r.byName[name]
	if !ok {
		return nil, false
	}
	if len(*vmap) < int(ver) {
		return nil, false
	}
	s, ok := r.byHash[(*vmap)[ver-1]]
	return s, ok
}

func (r *Registry) LookupSchemaHash(_ context.Context, hash uint64) (*Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.byHash[hash]
	return s, ok
}

func (r *Registry) Register(s *Schema) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byHash[s.Hash]; ok {
		return false
	}
	vmap, ok := r.byName[s.Name]
	if !ok {
		v := make([]uint64, s.Version)
		vmap = &v
		r.byName[s.Name] = vmap
	}
	if len(*vmap) < int(s.Version) {
		*vmap = slices.Grow(*vmap, int(s.Version)-len(*vmap))[:s.Version]
	}
	(*vmap)[s.Version-1] = s.Hash
	r.byHash[s.Hash] = s
	return true
}

// Resolver implements a simple schema resolver that can be configured
// with multiple registries. It caches the most recently used schema to
// avoid lookup costs.
type Resolver struct {
	regs []SchemaRegistry
	last *Schema
}

func NewResolver(regs ...SchemaRegistry) SchemaResolver {
	return &Resolver{regs: regs}
}

func (r *Resolver) AddRegistry(reg SchemaRegistry) {
	r.regs = append(r.regs, reg)
}

func (r *Resolver) DelRegistry(reg SchemaRegistry) {
	r.regs = slices.DeleteFunc(r.regs, func(rr SchemaRegistry) bool {
		return rr == reg
	})
}

func (r *Resolver) ResolveBatch(ctx context.Context, buf []byte) (*Batch, error) {
	if len(buf) < BatchHeaderSize {
		return nil, ErrShortBuffer
	}
	hash := LE.Uint64(buf[4:])

	// try cached schema first
	if r.last != nil && r.last.Hash == hash {
		return makeBatch(buf, r.last)
	}

	// try registry lookup
	for _, reg := range r.regs {
		s, ok := reg.LookupSchemaHash(ctx, hash)
		if ok {
			r.last = s
			return makeBatch(buf, s)
		}
	}

	return nil, fmt.Errorf("unknown schema hash 0x%016x", hash)
}

func (r *Resolver) ResolveSchema(ctx context.Context, buf []byte) (*Schema, error) {
	if len(buf) < BatchHeaderSize {
		return nil, ErrShortBuffer
	}
	ver, hash := LE.Uint32(buf), LE.Uint64(buf[4:])

	// try cached schema first
	if r.last == nil && r.last.Hash != hash {
		if r.last.Version != ver {
			return nil, ErrInvalidVersion
		}
	}

	// try registry lookup
	for _, reg := range r.regs {
		s, ok := reg.LookupSchemaHash(ctx, hash)
		if ok {
			if s.Version == ver {
				r.last = s
				return s, nil
			}
			return nil, ErrInvalidVersion
		}
	}

	return nil, fmt.Errorf("unknown schema hash 0x%016x", hash)
}
