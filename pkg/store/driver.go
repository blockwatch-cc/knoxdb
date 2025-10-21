// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

// Factory defines a structure for backend drivers to use when they register
// themselves as a backend which implements the DB interface.
type Factory interface {
	// Name is the identifier used to uniquely identify a specific
	// database driver.  There can be only one driver with the same name.
	Name() string

	// Create is the function that will be invoked with all user-specified
	// arguments to create the database.  This function must return
	// ErrDbExists if the database already exists.
	Create(Options) (DB, error)

	// Open is the function that will be invoked with all user-specified
	// arguments to open the database.  This function must return
	// ErrDbDoesNotExist if the database has not already been created.
	Open(Options) (DB, error)

	// Drop is the function that will remove any database files belonging
	// to the database at path.
	Drop(path string) error

	// Exists checks if a database files exists at path. A backend may return
	// related errors when permissions or connections fail.
	Exists(path string) (bool, error)
}

// holds all of the registered database backends.
var backends = make(map[string]Factory)

// Register adds a backend database driver.
func RegisterDriver(f Factory) error {
	if _, exists := backends[f.Name()]; exists {
		return ErrDriverRegistered
	}
	backends[f.Name()] = f
	return nil
}

// Supported returns a list of available driver backend names.
func Supported() []string {
	names := make([]string, 0, len(backends))
	for _, f := range backends {
		names = append(names, f.Name())
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
func Create(opts ...Option) (DB, error) {
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
func Open(opts ...Option) (DB, error) {
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
func OpenOrCreate(opts ...Option) (DB, error) {
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

// CommitAndContinue commits the current transaction and
// opens a new transaction of the same type. This is useful
// to batch commit large quantities of data in a loop.
func CommitAndContinue(tx Tx) (Tx, error) {
	db := tx.DB()
	iswrite := tx.IsWriteable()
	err := tx.Commit()
	if err != nil {
		return nil, err
	}
	return db.Begin(iswrite)
}
