// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// KnoxDB database inspector

package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/echa/log"

	pi "blockwatch.cc/knoxdb/internal/index/pack"
	"blockwatch.cc/knoxdb/internal/metadata"
	"blockwatch.cc/knoxdb/internal/pack"
	pt "blockwatch.cc/knoxdb/internal/table/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/jedib0t/go-pretty/v6/table"
)

var (
	flags      = flag.NewFlagSet("packview", flag.ContinueOnError)
	verbose    bool
	debug      bool
	trace      bool
	headRepeat int
	cmd        string = "table"
)

var cmdinfo = `
Available Commands:
  schema       show schema
  meta         show pack-level metadata
  detail       show block-level metadata
  content      show pack content
`

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&debug, "vv", false, "enable debug mode")
	flags.BoolVar(&trace, "vvv", false, "enable trace mode")
	flags.IntVar(&headRepeat, "head", 80, "repeat headers every `num` records")
}

func main() {
	defer func() {
		if e := recover(); e != nil {
			log.Error(e)
		}
	}()
	if err := run(); err != nil {
		log.Error(err)
	}
}

func printhelp() {
	fmt.Println("Usage:\n  packview [flags] [command] [path/database/table.db][#pack]")
	fmt.Println(cmdinfo)
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

func run() error {
	err := flags.Parse(os.Args[1:])
	if err != nil {
		if err == flag.ErrHelp {
			printhelp()
			return nil
		}
		return err
	}
	lvl := log.LevelInfo
	switch true {
	case trace:
		lvl = log.LevelTrace
	case debug:
		lvl = log.LevelDebug
	case verbose:
		lvl = log.LevelInfo
	}
	log.SetLevel(lvl)

	var n int
	if flags.NArg() > 1 {
		cmd = util.NonZero(flags.Arg(n), cmd)
		n++
	}
	desc := separateTarget(flags.Arg(n))

	if debug {
		fmt.Printf("cmd=%s\n", cmd)
		fmt.Printf("dir=%s\n", desc.Dir)
		fmt.Printf("db=%s\n", desc.Name)
		fmt.Printf("table=%s\n", desc.Table)
		fmt.Printf("id=%d\n", desc.PackId)
	}

	if cmd == "" {
		return fmt.Errorf("Missing command.")
	}
	if !desc.IsValid() {
		return fmt.Errorf("Invalid database file locator. Need [path/database/table.db][#pack]")
	}

	ctx := context.Background()
	opts := knox.ReadonlyDatabaseOptions.WithPath(desc.Dir).WithLogger(log.Log)
	db, err := knox.OpenDatabase(ctx, desc.Name, opts)
	if err != nil {
		return fmt.Errorf("opening database %s: %v", desc.Dir, err)
	}
	defer db.Close(ctx)

	out := io.Writer(os.Stdout)

	switch cmd {
	case "schema":
		return PrintSchema(getTableOrIndexSchema(db, desc.Table), out)
	case "meta":
		return PrintMetadata(getTableOrIndexMetadataView(db, desc.Table), desc.PackId, out)
	case "detail":
		return PrintMetadataDetail(getTableOrIndexMetadataView(db, desc.Table), desc.PackId, out)
	case "content":
		ctx, _, abort := db.Begin(ctx)
		defer abort()
		return PrintContent(ctx, getTableOrIndexPackView(db, desc.Table), desc.PackId, out)

	default:
		return fmt.Errorf("unsupported command %s", cmd)
	}
}

func getTableOrIndexSchema(db knox.Database, name string) *schema.Schema {
	t, err := db.UseTable(name)
	if err == nil {
		return t.Schema()
	}
	if idx, err := db.UseIndex(name); err == nil {
		return idx.Schema()
	}
	panic(err)
}

type ContentViewer interface {
	ViewPackage(context.Context, int) *pack.Package
	ViewTomb() bitmap.Bitmap
	Schema() *schema.Schema
}

func getTableOrIndexPackView(db knox.Database, name string) ContentViewer {
	t, err := db.UseTable(name)
	if err == nil {
		return t.Engine().(*pt.Table)
	}
	if idx, err := db.UseIndex(name); err == nil {
		return idx.Engine().(*pi.Index)
	}
	panic(err)
}

type MetadataViewer interface {
	ViewMetadata(int) *metadata.PackMetadata
	Schema() *schema.Schema
}

func getTableOrIndexMetadataView(db knox.Database, name string) MetadataViewer {
	t, err := db.UseTable(name)
	if err == nil {
		return t.Engine().(*pt.Table)
	}
	if idx, err := db.UseIndex(name); err == nil {
		return idx.Engine().(*pi.Index)
	}
	panic(err)
}

// Takes target descriptor and splits it into components
// returns path, name of the database, name of a table and an
// array of optional pack descriptors
type TableDescriptor struct {
	Dir    string
	Name   string
	Table  string
	PackId int
}

func (d TableDescriptor) IsValid() bool {
	return d.Dir != "" && d.Name != "" && d.Table != ""
}

func separateTarget(s string) TableDescriptor {
	path, extra, _ := strings.Cut(s, "#")
	dbPath, fileName := filepath.Split(filepath.Clean(path))
	desc := TableDescriptor{
		Dir:    filepath.Dir(filepath.Clean(dbPath)),
		Name:   filepath.Base(filepath.Clean(dbPath)),
		Table:  strings.TrimSuffix(filepath.Base(fileName), ".db"),
		PackId: -1,
	}
	switch {
	case extra == "journal":
		desc.PackId = int(pack.JournalKeyId)
	case extra == "tomb":
		desc.PackId = int(pack.TombstoneKeyId)
	default:
		if n, err := strconv.ParseInt(extra, 0, 64); err == nil {
			desc.PackId = int(n)
		}
	}
	return desc
}

func PrintSchema(s *schema.Schema, w io.Writer) error {
	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.SetTitle("Schema %s [0x%x] - %d fields - %d bytes", s.Name(), s.Hash(), s.NumFields(), s.WireSize())
	t.AppendHeader(table.Row{"#", "Name", "Type", "Flags", "Index", "Visible", "Array", "Scale", "Size", "Fixed", "Compress"})
	for _, field := range s.Exported() {
		t.AppendRow([]any{
			field.Id,
			field.Name,
			field.Type,
			field.Flags,
			field.Index,
			field.IsVisible,
			field.IsArray,
			field.Scale,
			field.Type.Size(),
			field.Fixed,
			field.Compress,
		})
	}
	t.Render()
	return nil
}

func PrintMetadata(view MetadataViewer, id int, w io.Writer) error {
	s := view.Schema()
	pki := s.PkIndex()
	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.SetTitle("%s - %d fields", s.Name(), s.NumFields())
	t.AppendHeader(table.Row{"#", "Key", "Blocks", "Records", "MinPk", "MaxPk", "Size"})
	var (
		i, n      int
		stopAfter bool
	)
	if id >= 0 {
		i = id
		stopAfter = true
	}
	for {
		md := view.ViewMetadata(i)
		if md == nil {
			break
		}
		t.AppendRow([]any{
			n + 1,
			fmt.Sprintf("%08x", md.Key),
			len(md.Blocks),
			md.NValues,
			md.Blocks[pki].MinValue.(uint64),
			md.Blocks[pki].MaxValue.(uint64),
			util.ByteSize(md.StoredSize),
		})
		i++
		n++
		if stopAfter {
			break
		}
	}
	t.Render()
	return nil
}

func PrintMetadataDetail(view MetadataViewer, id int, w io.Writer) error {
	s := view.Schema()
	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.AppendHeader(table.Row{"#", "Type", "Cardinality", "Min", "Max", "Size", "Bloom", "Bits"})
	var (
		i         int
		stopAfter bool
	)
	if id >= 0 {
		i = id
		stopAfter = true
	}
	for {
		md := view.ViewMetadata(i)
		if md == nil {
			break
		}
		t.SetTitle("%s - Pack 0x%08x (%d) - %s records - Size %s - Meta %s",
			s.Name(),
			md.Key,
			md.Key,
			util.PrettyInt(md.NValues),
			util.ByteSize(md.StoredSize),
			util.ByteSize(md.HeapSize()),
		)
		for id, binfo := range md.Blocks {
			bloomSz, bitSz := "--", "--"
			if binfo.Bloom != nil {
				bloomSz = util.ByteSize(int(binfo.Bloom.Len() / 8)).String()
			}
			if binfo.Bits != nil {
				bitSz = util.ByteSize(len(binfo.Bits.ToBuffer())).String()
			}
			t.AppendRow([]any{
				id + 1,
				binfo.Type,
				util.PrettyInt(binfo.Cardinality),
				util.LimitStringEllipsis(util.ToString(binfo.MinValue), 33),
				util.LimitStringEllipsis(util.ToString(binfo.MaxValue), 33),
				util.ByteSize(binfo.StoredSize),
				bloomSz,
				bitSz,
			})
		}
		t.Render()
		t.ResetRows()
		i++
		if stopAfter {
			break
		}
	}
	return nil
}

func PrintContent(ctx context.Context, view ContentViewer, id int, w io.Writer) error {
	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.SetPageSize(headRepeat)
	s := view.Schema()

	// analyze schema and set custom text transformer for byte and enum columns
	var cfgs []table.ColumnConfig
	for _, field := range s.Exported() {
		if field.Type == types.FieldTypeBytes {
			cfgs = append(cfgs, table.ColumnConfig{
				Name: field.Name,
				Transformer: func(val any) string {
					return hex.EncodeToString(val.([]byte))
				},
			})
		}
		if field.Type == types.FieldTypeUint16 && field.Flags.Is(types.FieldFlagEnum) {
			if lut, err := schema.LookupEnum(field.Name); err == nil {
				cfgs = append(cfgs, table.ColumnConfig{
					Name: field.Name,
					Transformer: func(val any) string {
						enum, ok := lut.Value(val.(uint16))
						if ok {
							return string(enum)
						}
						return strconv.Itoa(int(val.(uint16)))
					},
				})

			}
		}
	}
	if cfgs != nil {
		t.SetColumnConfigs(cfgs)
	}

	// handle journal separate (add deleted column)
	var res []any
	if id == int(pack.JournalKeyId) {
		pkg := view.ViewPackage(ctx, id)
		tomb := view.ViewTomb()
		pki := s.PkIndex()
		t.AppendHeader(append(table.Row{"DEL"}, util.StringList(s.FieldNames()).AsInterface()...))
		for r := 0; r < pkg.Len(); r++ {
			res = pkg.ReadRow(r, res)
			var live string
			if tomb.Contains(res[pki].(uint64)) {
				live = "*"
			}
			t.AppendRow(append([]any{live}, res...))
		}
		t.Render()
		t.ResetRows()
		t.ResetHeaders()
		return nil
	}

	// regular data packs
	var (
		i         int
		stopAfter bool
	)
	if id >= 0 {
		i = id
		stopAfter = true
	}
	t.AppendHeader(util.StringList(s.FieldNames()).AsInterface())
	for {
		pkg := view.ViewPackage(ctx, i)
		if pkg == nil {
			if stopAfter {
				panic(fmt.Errorf("pack %d not found", id))
			}
			break
		}
		for r := 0; r < pkg.Len(); r++ {
			t.AppendRow(pkg.ReadRow(r, nil))
		}
		pkg.Release()
		t.Render()
		t.ResetRows()
		i++
		if stopAfter {
			break
		}
	}
	return nil
}
