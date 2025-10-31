// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/echa/log"
)

// TODO
// - tomb should not have to store extra fields
// - data placement algorithm is inefficient for hash indexes (use linear hashing)

// This index supports the following condition types on lookup.
// - hash: EQ, IN (single or composite EQ)
// - int:  EQ, LT, LE GT, GE, RG (single condition)

var _ engine.IndexEngine = (*Index)(nil)

// key extraction from wire format is little endian
var LE = binary.LittleEndian

func init() {
	engine.RegisterIndexFactory(engine.IndexKindPack, NewIndex)
}

var (
	DefaultIndexOptions = engine.IndexOptions{
		Driver:      "bolt",
		PackSize:    1 << 11, // 2k
		JournalSize: 1 << 17, // 128k
		PageSize:    1 << 16,
		PageFill:    0.5,
		TxMaxSize:   1 << 20, // 1 MB
		ReadOnly:    false,
		NoSync:      true,
		Logger:      log.Disabled,
	}
)

type Index struct {
	engine  *engine.Engine      // engine access
	sindex  *schema.IndexSchema // index schema spec
	sstore  *schema.Schema      // on-disk storage schema
	id      uint64              // unique tagged name hash
	name    string              // index name (for logging)
	opts    engine.IndexOptions // copy of config options
	table   engine.TableEngine  // related table
	state   engine.ObjectState  // volatile state
	db      store.DB            // storage backend
	journal *pack.Package       // in-memory updates
	tomb    *pack.Package       // in-memory deletes
	convert Converter           // table to index schema converter
	metrics engine.IndexMetrics // usage statistics
	log     log.Logger          // log instance
}

func NewIndex() engine.IndexEngine {
	return &Index{}
}

func (idx *Index) IsReadOnly() bool {
	return idx.opts.ReadOnly
}

func (idx *Index) Create(ctx context.Context, t engine.TableEngine, s *schema.IndexSchema, opts engine.IndexOptions) error {
	// require primary key
	if !s.Base.HasMeta() {
		return engine.ErrNoMeta
	}

	// init names
	opts = DefaultIndexOptions.Merge(opts)

	// storage schema depends on index type
	sout, conv, err := convertSchema(s)
	if err != nil {
		return err
	}

	// setup index
	idx.engine = engine.GetEngine(ctx)
	idx.sindex = s
	idx.sstore = sout
	idx.id = s.TaggedHash(types.ObjectTagIndex)
	idx.name = s.Name
	idx.opts = opts
	idx.table = t
	idx.state = engine.NewObjectState(s.Name)
	idx.journal = pack.New().
		WithMaxRows(opts.JournalSize).
		WithSchema(sout).
		Alloc()
	idx.tomb = pack.New().
		WithMaxRows(opts.JournalSize).
		WithSchema(sout).
		Alloc()
	idx.convert = conv
	idx.metrics = engine.NewIndexMetrics(s.Name)
	idx.log = idx.opts.Logger.WithTag(fmt.Sprintf("index[%s]:", s.Name))

	// create backend and store initial state
	if err := idx.createBackend(ctx); err != nil {
		return err
	}

	idx.log.Debugf("create successful")
	return nil
}

func (idx *Index) createBackend(ctx context.Context) error {
	// setup backend db file
	path := filepath.Join(idx.engine.RootPath(), idx.name)
	idx.log.Debugf("creating type=%s backend=%s table=%s path=%q opts=%#v",
		idx.sindex.Type, idx.opts.Engine, idx.table.Schema().Name, path, idx.opts)

	opts := append(
		idx.opts.StoreOptions(),
		store.WithLogger(idx.log),
		store.WithPath(path),
		store.WithManifest(
			store.NewManifest(
				idx.name,
				idx.engine.Namespace()+"."+idx.sstore.Label(),
			),
		),
	)
	db, err := store.Create(opts...)
	if err != nil {
		return fmt.Errorf("creating index %s: %v", idx.name, err)
	}
	idx.db = db

	// init table storage
	tx, err := idx.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range [][]byte{
		engine.DataKeySuffix,
		engine.TombKeySuffix,
		engine.StateKeySuffix,
	} {
		key := append([]byte(idx.name), v...)
		if _, err := tx.Root().CreateBucket(key); err != nil {
			return err
		}
	}

	// init and store state
	if err := idx.state.Store(ctx, tx); err != nil {
		return err
	}

	// commit backend tx
	return tx.Commit()
}

func (idx *Index) Open(ctx context.Context, t engine.TableEngine, s *schema.IndexSchema, opts engine.IndexOptions) error {
	// storage schema depends on index type
	sout, conv, err := convertSchema(s)
	if err != nil {
		return err
	}

	// setup index
	idx.engine = engine.GetEngine(ctx)
	idx.sindex = s
	idx.sstore = sout
	idx.id = s.TaggedHash(types.ObjectTagIndex)
	idx.name = s.Name
	idx.opts = DefaultIndexOptions.Merge(opts)
	idx.table = t
	idx.state = engine.NewObjectState(s.Name)
	idx.journal = pack.New().
		WithMaxRows(opts.JournalSize).
		WithSchema(sout).
		Alloc()
	idx.tomb = pack.New().
		WithMaxRows(opts.JournalSize).
		WithSchema(sout).
		Alloc()
	idx.convert = conv
	idx.metrics = engine.NewIndexMetrics(s.Name)
	idx.log = idx.opts.Logger.WithTag(fmt.Sprintf("index[%s]:", s.Name))

	// open db backend and load latest state
	if err := idx.openBackend(ctx); err != nil {
		idx.log.Error(err)
		_ = idx.Close(ctx)
		return engine.ErrDatabaseCorrupt
	}

	// try GC old epochs
	if !idx.IsReadOnly() {
		if err := idx.Cleanup(ctx, uint32(idx.state.Epoch)); err != nil {
			idx.log.Warn(err)
		}
	}

	idx.log.Debugf("opened with %d entries", idx.state.NRows)

	return nil
}

func (idx *Index) openBackend(ctx context.Context) error {
	// open backend
	path := filepath.Join(idx.engine.RootPath(), idx.name)
	idx.log.Debugf("open type=%s backend=%s path=%s opts=%#v",
		idx.sindex.Type, idx.opts.Engine, path, idx.opts)

	opts := append(
		idx.opts.StoreOptions(),
		store.WithLogger(idx.log),
		store.WithPath(path),
		store.WithManifest(
			store.NewManifest(
				idx.name,
				idx.engine.Namespace()+"."+idx.sstore.Label(),
			),
		),
	)
	db, err := store.Open(opts...)
	if err != nil {
		return fmt.Errorf("open: %v", err)
	}
	idx.db = db

	// load index state
	err = idx.db.View(func(tx store.Tx) error {
		for _, v := range [][]byte{
			engine.DataKeySuffix,
			engine.TombKeySuffix,
			engine.StateKeySuffix,
		} {
			key := append([]byte(idx.name), v...)
			if tx.Bucket(key) == nil {
				return fmt.Errorf("%q: %v", string(key), store.ErrBucketNotFound)
			}
		}

		if err := idx.state.Load(ctx, tx); err != nil {
			return fmt.Errorf("loading state: %v", err)
		}
		idx.metrics.TupleCount = int64(idx.state.NRows)

		return nil
	})
	return err
}

func (idx *Index) Close(ctx context.Context) (err error) {
	if idx.db != nil {
		idx.log.Debug("closing")
		err = idx.db.Close()
		idx.db = nil
	}
	idx.engine = nil
	idx.sindex = nil
	idx.sstore = nil
	idx.table = nil
	idx.id = 0
	idx.db = nil
	idx.opts = engine.IndexOptions{}
	idx.metrics = engine.IndexMetrics{}
	idx.convert = nil
	idx.state.Reset()
	idx.journal.Release()
	idx.tomb.Release()
	idx.journal = nil
	idx.tomb = nil
	return
}

func (idx *Index) IndexSchema() *schema.IndexSchema {
	return idx.sindex
}

func (idx *Index) Schema() *schema.Schema {
	return idx.sstore
}

func (idx *Index) Table() engine.TableEngine {
	return idx.table
}

func (idx *Index) IsComposite() bool {
	return idx.sindex.Type == types.IndexTypeComposite
}

func (idx *Index) IsPk() bool {
	return idx.sindex.Type == types.IndexTypePk
}

func (idx *Index) Sync(ctx context.Context) error {
	return idx.db.Sync()
}

func (idx *Index) Metrics() engine.IndexMetrics {
	m := idx.metrics
	m.TupleCount = int64(idx.state.NRows)
	m.TotalSize = int64(idx.state.NextRid)
	m.PacksCount = int64(idx.state.NextPk)
	return m
}

func (idx *Index) Drop(ctx context.Context) error {
	path := idx.db.Path()
	idx.db.Close()
	idx.db = nil
	idx.log.Debugf("dropping file %s", path)
	return store.Drop(idx.opts.Driver, path)
}

func (idx *Index) Truncate(ctx context.Context) error {
	// start direct backend write tx (assumes index and table are
	// not stored in the same backend db file)
	err := idx.db.Update(func(tx store.Tx) error {
		for _, v := range [][]byte{
			engine.DataKeySuffix,
			engine.TombKeySuffix,
			engine.StateKeySuffix,
		} {
			key := append([]byte(idx.name), v...)
			if err := tx.Root().DeleteBucket(key); err != nil {
				return err
			}
			if _, err := tx.Root().CreateBucket(key); err != nil {
				return err
			}
		}
		// reset state
		idx.state.Reset()
		return idx.state.Store(ctx, tx)
	})
	if err != nil {
		return err
	}

	// clear data
	idx.journal.Clear()
	idx.tomb.Clear()

	// update metrics
	idx.metrics.TotalSize = 0
	idx.metrics.PacksCount = 0
	idx.metrics.DeletedTuples = 0
	idx.metrics.TupleCount = 0

	return nil
}

func (idx *Index) Rebuild(ctx context.Context) error {
	// walk all table packs
	rd := idx.table.NewReader().WithFields(idx.sindex.Ids())
	defer rd.Close()

	for {
		// read next table pack
		pkg, err := rd.Next(ctx)
		if err != nil {
			return err
		}

		// stop when we reached the end of the table
		if pkg == nil {
			break
		}

		// add pack contents to index
		if err := idx.AddPack(ctx, pkg, pack.WriteModeAll); err != nil {
			return err
		}
	}

	// final index flush
	return idx.Finalize(ctx, 0)
}

func (idx *Index) AddPack(ctx context.Context, pkg *pack.Package, mode pack.WriteMode) error {
	// idx.log.Debugf("index[%s]: add journal epoch %d to j[%d:%d]", idx.name, pkg.Key(),
	// 	idx.journal.Len(), idx.journal.Cap())

	// build new index pack, relink columns and/or produce `hash` column)
	ipkg := idx.convert.ConvertPack(pkg, mode)
	ipkg.WithSelection(pkg.Selected())

	var state pack.AppendState

	for {
		// append next chunk of data to journal: max(cap(journal), len(src))
		_, state = idx.journal.AppendSelected(ipkg, mode, state)

		// store when journal is full
		if idx.journal.IsFull() {
			if err := idx.mergeAppend(ctx); err != nil {
				idx.log.Debugf("merge failed: %v", err)
				return err
			}
		}

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}
	ipkg.WithSelection(nil)
	ipkg.Release()

	return nil
}

func (idx *Index) DelPack(ctx context.Context, pkg *pack.Package, mode pack.WriteMode, epoch uint32) error {
	// idx.log.Debugf("index[%s]: del journal epoch %d", idx.idxSchema.Name(), pkg.Key())

	// build new index pack, relink columns and produce `hash` column)
	ipkg := idx.convert.ConvertPack(pkg, mode)
	ipkg.WithSelection(pkg.Selected())

	var state pack.AppendState
	for {
		// append next chunk of data to tomb: max(cap(tomb), len(src))
		_, state = idx.tomb.AppendSelected(ipkg, mode, state)

		// store when tomb is full
		if idx.tomb.IsFull() {
			if err := idx.storeTomb(ctx, epoch); err != nil {
				return err
			}
		}

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}

	ipkg.WithSelection(nil)
	ipkg.Release()

	return nil
}

func (idx *Index) Finalize(ctx context.Context, epoch uint32) error {
	// flush remaining journal entries
	if idx.journal.Len() > 0 {
		if err := idx.mergeAppend(ctx); err != nil {
			return err
		}
	}

	// write tombstone for later GC
	if idx.tomb.Len() > 0 {
		if err := idx.storeTomb(ctx, epoch); err != nil {
			return err
		}
	}

	// write epoch
	err := idx.db.Update(func(tx store.Tx) error {
		idx.state.Epoch = uint64(epoch)
		return idx.state.Store(ctx, tx)
	})
	if err != nil {
		return err
	}

	// sync db file if running in no-sync write mode
	if idx.opts.NoSync {
		if err := idx.Sync(ctx); err != nil {
			return err
		}
	}

	// reset tomb pack id
	idx.tomb.WithKey(0)

	return nil
}

func (idx *Index) GC(ctx context.Context, epoch uint32) error {
	idx.log.Debugf("gc epoch %d", epoch)
	var key uint32
	for {
		pkg, err := idx.loadTomb(ctx, key, epoch)
		if err != nil {
			return err
		}
		if pkg == nil {
			break
		}
		if err := idx.mergeTomb(ctx, pkg); err != nil {
			return err
		}
		pkg.Release()
		if err := idx.dropTomb(ctx, key, epoch); err != nil {
			return err
		}
		key++
	}
	return nil
}

// GC all tombstones <= epoch. Called in startup
func (idx *Index) Cleanup(ctx context.Context, epoch uint32) error {
	idx.log.Debugf("cleanup until epoch %d", epoch)
	var drop []uint32
	err := idx.db.View(func(tx store.Tx) error {
		b := tx.Bucket(append([]byte(idx.name), engine.TombKeySuffix...))
		if b == nil {
			return store.ErrBucketNotFound
		}
		c := b.Cursor()
		defer c.Close()
		if !c.First() {
			return nil
		}

		// decode version (Note: block key uses 16bit stripped epoch version)
		_, v, _ := pack.DecodeBlockKey(c.Key())
		for e := v; e <= epoch&0xFFFF; e++ {
			drop = append(drop, e)
		}

		return nil
	})
	if err != nil {
		return err
	}
	if len(drop) == 0 {
		return nil
	}

	idx.log.Debugf("gc %d epochs", len(drop))
	for _, e := range drop {
		if err := idx.GC(ctx, e); err != nil {
			return fmt.Errorf("gc epoch %d: %v", e, err)
		}
	}

	return nil
}
