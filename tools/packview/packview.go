// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// KnoxDB database inspector

package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	dbg "runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/echa/log"

	"blockwatch.cc/knoxdb/internal/pack"
	pi "blockwatch.cc/knoxdb/internal/pack/index"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	pt "blockwatch.cc/knoxdb/internal/pack/table"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/encode"
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
  stats        show pack-level statistics
  detail       show block-level statistics
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
	if err := run(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func printhelp() {
	fmt.Println("Usage:\n  packview [flags] [command] [path/database/table.db][#pack]")
	fmt.Println(cmdinfo)
	fmt.Println("Flags:")
	flags.PrintDefaults()
	fmt.Println()
}

func run() (err error) {
	defer func() {
		if e := recover(); e != nil {
			log.Error(e)
			dbg.PrintStack()
			switch x := e.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown panic")
			}
		}
	}()
	err = flags.Parse(os.Args[1:])
	if err != nil {
		if err == flag.ErrHelp {
			printhelp()
			return nil
		}
		return err
	}
	lvl := log.LevelInfo
	switch {
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
		PrintSchema(getTableOrIndexSchema(db, desc.Table), out)
	case "stats":
		PrintMetadata(getTableOrIndexStatsView(db, desc.Table), desc, out)
	case "detail":
		PrintDetail(getTableOrIndexView(db, desc.Table), desc, out)
	case "content":
		ctx, _, abort, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		defer abort()
		PrintContent(ctx, getTableOrIndexView(db, desc.Table), desc, out)

	default:
		return fmt.Errorf("unsupported command %s", cmd)
	}
	return nil
}

//nolint:all
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
	ViewTomb() *xroar.Bitmap
	Schema() *schema.Schema
}

type StatsViewer interface {
	ViewStats(int) *stats.Record
	Schema() *schema.Schema
}

type Viewer interface {
	StatsViewer
	ContentViewer
}

//nolint:all
func getTableOrIndexView(db knox.Database, name string) Viewer {
	t, err := db.UseTable(name)
	if err == nil {
		return t.Engine().(*pt.Table)
	}
	if idx, err := db.UseIndex(name); err == nil {
		return idx.Engine().(*pi.Index)
	}
	panic(err)
}

//nolint:all
func getTableOrIndexStatsView(db knox.Database, name string) StatsViewer {
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
	Dir        string
	Name       string
	Table      string
	PackId     int
	HavePackId bool
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
		PackId: 0,
	}
	switch {
	case extra == "journal":
		desc.PackId = -1
		desc.HavePackId = true
	// case extra == "tomb":
	// 	desc.PackId = -2
	default:
		if n, err := strconv.ParseInt(extra, 0, 64); err == nil {
			desc.PackId = int(n)
			desc.HavePackId = true
		}
	}
	return desc
}

func PrintSchema(s *schema.Schema, w io.Writer) {
	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.SetTitle("Schema %s [0x%x] - %d fields - %d bytes", s.Name(), s.Hash(), s.NumFields(), s.WireSize())
	t.AppendHeader(table.Row{"#", "Name", "Type", "Flags", "Index", "Size", "Compress"})
	for _, f := range s.Exported() {
		var (
			typ    string
			findex string
		)
		switch f.Type {
		case schema.FT_TIME, schema.FT_TIMESTAMP:
			typ = f.Type.String() + "(" + schema.TimeScale(f.Scale).ShortName() + ")"
		case schema.FT_D32, schema.FT_D64, schema.FT_D128, schema.FT_D256:
			typ = f.Type.String() + "(" + strconv.Itoa(int(f.Scale)) + ")"
		case schema.FT_STRING, schema.FT_BYTES:
			if f.Fixed > 0 {
				typ = "[" + strconv.Itoa(int(f.Fixed)) + "]" + f.Type.String()
			}
		}
		if typ == "" {
			typ = f.Type.String()
		}
		if f.Index > 0 {
			findex = f.Index.String() + ":" + strconv.Itoa(int(f.Scale))
		}
		t.AppendRow([]any{
			f.Id,
			f.Name,
			typ,
			f.Flags.String(),
			findex,
			f.Type.Size(),
			f.Compress,
		})
	}
	t.Render()
}

func PrintMetadata(view StatsViewer, desc TableDescriptor, w io.Writer) {
	s := view.Schema()
	rx := s.RowIdIndex()
	t := table.NewWriter()
	t.SetPageSize(headRepeat)
	t.SetOutputMirror(w)
	t.SetTitle("%s - %d fields - #%016x", s.Name(), s.NumFields(), s.Hash())
	t.AppendHeader(table.Row{"#", "Key", "Version", "Records", "RID min", "RID max", "Size"})
	var (
		i, n      int
		stopAfter bool
	)
	if desc.HavePackId {
		i = desc.PackId
		stopAfter = true
	}
	for {
		md := view.ViewStats(i)
		if md == nil {
			break
		}
		t.AppendRow([]any{
			n + 1,
			fmt.Sprintf("%08x", md.Key),
			md.Version,
			md.NValues,
			md.Min(rx),
			md.Max(rx),
			util.ByteSize(md.DiskSize),
		})
		i++
		n++
		if stopAfter {
			break
		}
	}
	t.Render()
}

type InfoView interface {
	Type() encode.ContainerType
	Info() string
	Size() int
}

func PrintDetail(view Viewer, desc TableDescriptor, w io.Writer) {
	s := view.Schema()
	t := table.NewWriter()
	fields := s.Exported()
	t.SetOutputMirror(w)
	t.AppendHeader(table.Row{"#", "Name", "Type", "Min", "Max", "Size", "Info"})
	var (
		i         int
		stopAfter bool
	)
	if desc.HavePackId {
		i = desc.PackId
		stopAfter = true
	}
	for {
		md := view.ViewStats(i)
		if md == nil {
			break
		}
		pkg := view.ViewPackage(context.Background(), i)
		if pkg == nil {
			break
		}
		t.SetTitle("%s - Pack 0x%08x[v%d] - %s records - Size %s",
			s.Name(),
			md.Key,
			md.Version,
			util.PrettyInt(int(md.NValues)),
			util.ByteSize(md.DiskSize),
		)
		for i := range s.NumFields() {
			var (
				sz   int
				info string
			)
			if pkg.Block(i).IsMaterialized() {
				sz = pkg.Block(i).Size()
				info = "raw"
			} else {
				v := pkg.Block(i).Container().(InfoView)
				sz = v.Size()
				info = v.Info()
			}

			t.AppendRow([]any{
				fields[i].Id,
				fields[i].Name,
				fields[i].Type,
				printValue(s, fields[i], md.Min(i)),
				printValue(s, fields[i], md.Max(i)),
				sz,
				info,
			})
		}
		t.Render()
		t.ResetRows()
		i++
		if stopAfter {
			break
		}
	}
}

func printValue(s *schema.Schema, f *schema.ExportedField, val any) any {
	switch f.Type {
	case types.FieldTypeBytes:
		return util.LimitStringEllipsis(util.ToString(val), 33)
	case types.FieldTypeUint16:
		if f.Flags.Is(types.FieldFlagEnum) && s.HasEnums() {
			if lut, ok := s.Enums().Lookup(f.Name); ok {
				enum, ok := lut.Value(val.(uint16))
				if ok {
					return enum
				}
			}
		}
		return val
	case types.FieldTypeTimestamp, types.FieldTypeDate, types.FieldTypeTime:
		return schema.TimeScale(f.Scale).Format(val.(time.Time))
	case types.FieldTypeInt128, types.FieldTypeInt256, types.FieldTypeDecimal128, types.FieldTypeDecimal256:
		return util.LimitStringEllipsis(val.(fmt.Stringer).String(), 33)
	default:
		return val
	}
}

func PrintContent(ctx context.Context, view ContentViewer, desc TableDescriptor, w io.Writer) {
	t := table.NewWriter()
	t.SetOutputMirror(w)
	t.SetPageSize(headRepeat)
	s := view.Schema()

	// analyze schema and set custom text transformer for byte and enum columns
	var cfgs []table.ColumnConfig
	for _, field := range s.Exported() {
		switch field.Type {
		case types.FieldTypeBytes:
			cfgs = append(cfgs, table.ColumnConfig{
				Name: field.Name,
				Transformer: func(val any) string {
					return hex.EncodeToString(val.([]byte))
				},
			})
		case types.FieldTypeUint16:
			if field.Flags.Is(types.FieldFlagEnum) && s.HasEnums() {
				if lut, ok := s.Enums().Lookup(field.Name); ok {
					cfgs = append(cfgs, table.ColumnConfig{
						Name: field.Name,
						Transformer: func(val any) string {
							enum, ok := lut.Value(val.(uint16))
							if ok {
								return enum
							}
							return strconv.Itoa(int(val.(uint16)))
						},
					})
				}
			}
		case types.FieldTypeTimestamp, types.FieldTypeDate, types.FieldTypeTime:
			cfgs = append(cfgs, table.ColumnConfig{
				Name: field.Name,
				Transformer: func(val any) string {
					return schema.TimeScale(field.Scale).Format(val.(time.Time))
				},
			})
		}
	}
	if cfgs != nil {
		t.SetColumnConfigs(cfgs)
	}

	// handle journal separate (add deleted column)
	var res []any
	if desc.PackId < 0 {
		pkg := view.ViewPackage(ctx, desc.PackId)
		tomb := view.ViewTomb()
		rx := s.RowIdIndex()
		t.AppendHeader(append(table.Row{"DEL"}, util.StringList(s.AllFieldNames()).AsInterface()...))
		for r := 0; r < pkg.Len(); r++ {
			res = pkg.ReadRow(r, res)
			var live string
			if tomb.Contains(res[rx].(uint64)) {
				live = "*"
			}
			t.AppendRow(append([]any{live}, res...))
		}
		t.Render()
		t.ResetRows()
		t.ResetHeaders()
		return
	}

	// regular data packs
	var (
		i         int
		stopAfter bool
	)
	if desc.HavePackId {
		i = desc.PackId
		stopAfter = true
	}
	t.AppendHeader(util.StringList(s.AllFieldNames()).AsInterface())
	for {
		pkg := view.ViewPackage(ctx, i)
		if pkg == nil {
			if stopAfter {
				panic(fmt.Errorf("pack %d not found", i))
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
}
