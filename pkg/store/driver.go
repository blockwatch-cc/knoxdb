// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

// Factory defines the interface required for store backend drivers.
type Factory interface {
	// Type is a unique database driver name. There can be only one
	// registred driver with the same name.
	Type() string

	// Create is called to create a database instance with given
	// options. This function must return ErrDbExists if the database
	// already exists.
	Create(Options) (DBManager, error)

	// Open is called to open a database instance with given options.
	// This function must return ErrDbDoesNotExist if the database has
	// not already been created.
	Open(Options) (DBManager, error)

	// Drop is called to remove the database at path from backend storage.
	Drop(path string) error

	// Exists checks if a database exists at path. A backend may return
	// permission or connection errors.
	Exists(path string) (bool, error)
}

// holds all of the registered database backends.
var backends = make(map[string]Factory)

// Register adds a backend database driver.
func RegisterDriver(f Factory) error {
	if _, exists := backends[f.Type()]; exists {
		return ErrDriverRegistered
	}
	backends[f.Type()] = f
	return nil
}

// Supported returns a list of available driver backend names.
func Supported() []string {
	names := make([]string, 0, len(backends))
	for _, f := range backends {
		names = append(names, f.Type())
	}
	return names
}

// IsSupported returns if the named driver is supported.
func IsSupported(drv string) bool {
	_, ok := backends[drv]
	return ok
}

func lookup(name string) (Factory, error) {
	f, exists := backends[name]
	if !exists {
		return nil, ErrDriverUnknown
	}
	return f, nil
}

// Create initializes and opens a database of the specified backend type.
// Options are backend specific.
func Create(opts ...Option) (DBManager, error) {
	cfg := defaultOptions()
	for _, v := range opts {
		if err := v(&cfg); err != nil {
			return nil, err
		}
	}
	drv, err := lookup(cfg.Driver)
	if err != nil {
		return nil, err
	}
	return drv.Create(cfg)
}

// Open opens an existing database of the specified backend type.
// Options are backend specific.
func Open(opts ...Option) (DBManager, error) {
	cfg := defaultOptions()
	for _, v := range opts {
		if err := v(&cfg); err != nil {
			return nil, err
		}
	}
	drv, err := lookup(cfg.Driver)
	if err != nil {
		return nil, err
	}
	return drv.Open(cfg)
}

// OpenOrCreate is a helper that opens a database when it exists
// of creates it otherwise.
func OpenOrCreate(opts ...Option) (DBManager, error) {
	cfg := defaultOptions()
	for _, v := range opts {
		if err := v(&cfg); err != nil {
			return nil, err
		}
	}
	drv, err := lookup(cfg.Driver)
	if err != nil {
		return nil, err
	}
	exists, err := drv.Exists(cfg.Path)
	if err != nil {
		return nil, err
	}
	if exists {
		return drv.Open(cfg)
	} else {
		return drv.Create(cfg)
	}
}

func Drop(driver string, path string) error {
	drv, err := lookup(driver)
	if err != nil {
		return err
	}
	return drv.Drop(path)
}

func Exists(driver string, path string) (bool, error) {
	drv, err := lookup(driver)
	if err != nil {
		return false, err
	}
	return drv.Exists(path)
}
