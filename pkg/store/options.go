// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

import (
	"github.com/echa/log"
)

type Option func(cfg *Options) error

type Options struct {
	Driver          string
	Path            string
	Manifest        *Manifest
	Log             log.Logger
	Readonly        bool
	PageSize        int
	InitialMmapSize int
	MmapFlags       int
	PageFill        float64
	NoSync          bool // skip fsync on commit (dangerous)
	NoGrowSync      bool // skip fsync+alloc on grow
	KeepOnClose     bool
	GetCallback     func(k, v []byte) []byte
	PutCallback     func(k, v []byte) ([]byte, []byte, error)
	DeleteCallback  func(k []byte) ([]byte, error)
}

func defaultOptions() Options {
	return Options{
		Driver:   "mem",
		Path:     "./db",
		Log:      log.Disabled,
		PageFill: 0.5,
	}
}

func WithDriver(d string) Option {
	return func(cfg *Options) error {
		cfg.Driver = d
		return nil
	}
}

func WithPath(p string) Option {
	return func(cfg *Options) error {
		cfg.Path = p
		return nil
	}
}

func WithManifest(m *Manifest) Option {
	return func(cfg *Options) error {
		cfg.Manifest = m
		return nil
	}
}

func WithLogger(l log.Logger) Option {
	return func(cfg *Options) error {
		cfg.Log = l
		return nil
	}
}

func WithReadonly() Option {
	return func(cfg *Options) error {
		cfg.Readonly = true
		return nil
	}
}

func WithPageSize(n int) Option {
	return func(cfg *Options) error {
		cfg.PageSize = n
		return nil
	}
}

func WithInitialMmapSize(n int) Option {
	return func(cfg *Options) error {
		cfg.InitialMmapSize = n
		return nil
	}
}

func WithMmapFlags(n int) Option {
	return func(cfg *Options) error {
		cfg.MmapFlags = n
		return nil
	}
}

func WithPageFill(f float64) Option {
	return func(cfg *Options) error {
		cfg.PageFill = f
		return nil
	}
}

func WithNoSync() Option {
	return func(cfg *Options) error {
		cfg.NoSync = true
		return nil
	}
}

func WithNoGrowSync() Option {
	return func(cfg *Options) error {
		cfg.NoGrowSync = true
		return nil
	}
}

func WithKeepOnClose() Option {
	return func(cfg *Options) error {
		cfg.KeepOnClose = true
		return nil
	}
}

func WithGetCallback(fn func(k, v []byte) []byte) Option {
	return func(cfg *Options) error {
		cfg.GetCallback = fn
		return nil
	}
}

func WithPutCallback(fn func(k, v []byte) ([]byte, []byte, error)) Option {
	return func(cfg *Options) error {
		cfg.PutCallback = fn
		return nil
	}
}

func WithDeleteCallback(fn func(k []byte) ([]byte, error)) Option {
	return func(cfg *Options) error {
		cfg.DeleteCallback = fn
		return nil
	}
}
