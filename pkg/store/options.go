// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

import (
	"strings"

	"github.com/echa/log"
)

// default file extension
const dbFileExt = ".db"

type Option func(cfg *Options) error

type Options struct {
	Driver      string
	Path        string
	Manifest    *Manifest
	Log         log.Logger
	Readonly    bool
	PageSize    int
	PageFill    float64
	NoSync      bool
	KeepOnClose bool
}

func defaultOptions() Options {
	return Options{
		Driver:      "mem",
		Path:        "./db", // will append `mem.db`
		Log:         log.Disabled,
		PageFill:    0.5,
		KeepOnClose: false,
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
		if strings.HasSuffix(p, "/") {
			p += cfg.Driver
		}
		if !strings.HasSuffix(p, dbFileExt) {
			p += dbFileExt
		}
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
		if l != nil {
			cfg.Log = l
		}
		return nil
	}
}

func WithReadonly(b bool) Option {
	return func(cfg *Options) error {
		cfg.Readonly = b
		return nil
	}
}

func WithPageSize(n int) Option {
	return func(cfg *Options) error {
		if n > 0 {
			cfg.PageSize = n
		}
		return nil
	}
}

func WithPageFill(f float64) Option {
	return func(cfg *Options) error {
		if f > 0 {
			cfg.PageFill = f
		}
		return nil
	}
}

func WithNoSync(b bool) Option {
	return func(cfg *Options) error {
		cfg.NoSync = b
		return nil
	}
}

func WithDropOnClose(b bool) Option {
	return func(cfg *Options) error {
		cfg.KeepOnClose = !b
		return nil
	}
}
