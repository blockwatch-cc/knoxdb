// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"slices"
	"sync"
)

// SchemaRegistry defines an interface for schema registries
// that can be implemented by any host application. Based on
// use case and access patterns users must decide whether to
// implement one registry per application or dedicated registries
// per schema family. Registries may load schemas on demand.
type SchemaRegistry interface {
	// Registers a schema returning true on first insert.
	Register(s *Schema) bool

	// Searches a schema by name and version returning true when found.
	Lookup(name string, ver uint32) (*Schema, bool)

	// Searches a schemy by hash and returns true when found.
	LookupHash(hash uint64) (*Schema, bool)
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

func (r *Registry) Lookup(name string, ver uint32) (*Schema, bool) {
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

func (r *Registry) LookupHash(hash uint64) (*Schema, bool) {
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
