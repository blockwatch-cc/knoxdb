// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// KnoxDB database inspector

package main

import (
	"bytes"
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
	"unicode/utf8"

	"github.com/echa/log"

	"blockwatch.cc/knoxdb/internal/pack"
	pi "blockwatch.cc/knoxdb/internal/pack/index"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	pt "blockwatch.cc/knoxdb/internal/pack/table"
	itypes "blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/encode"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

var (
	flags      = flag.NewFlagSet("packview", flag.ContinueOnError)
	verbose    bool
	debug      bool
	trace      bool
	headRepeat int
	cmd        string
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
			if trace {
				dbg.PrintStack()
			}
			switch x := e.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = fmt.Errorf("%v", e)
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
		if cmd == "" {
			cmd = flags.Arg(n)
		}
		n++
	} else {
		cmd = "schema"
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
		return fmt.Errorf("missing command")
	}
	if !desc.IsValid() {
		return fmt.Errorf("invalid database file locator: need [path/database/table.db][#pack]")
	}

	ctx := context.Background()
	opts := append(knox.NewReadOnlyOptions(),
		knox.WithPath(desc.Dir),
		knox.WithLogger(log.Log),
	)
	db, err := knox.OpenDatabase(ctx, desc.Name, opts...)
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
		PrintContent(ctx, getTableOrIndexView(db, desc.Table), desc, out)
	default:
		return fmt.Errorf("unsupported command %s", cmd)
	}
	return nil
}

//nolint:all
func getTableOrIndexSchema(db knox.Database, name string) *schema.Schema {
	t, err := db.FindTable(name)
	if err == nil {
		return t.Schema()
	}
	if idx, err := db.FindIndex(name); err == nil {
		return idx.Schema()
	}
	panic(err)
}

type ContentViewer interface {
	ViewPackage(context.Context, int) *pack.Package
	ViewTomb(int) *xroar.Bitmap
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

type TableViewer struct {
	*pt.Table
}

func (t TableViewer) Schema() *schema.Schema {
	return t.Table.Schema().Base()
}

//nolint:all
func getTableOrIndexView(db knox.Database, name string) Viewer {
	t, err := db.FindTable(name)
	if err == nil {
		return TableViewer{t.Engine().(*pt.Table)}
	}
	if idx, err := db.FindIndex(name); err == nil {
		return idx.Engine().(*pi.Index)
	}
	panic(err)
}

//nolint:all
func getTableOrIndexStatsView(db knox.Database, name string) StatsViewer {
	t, err := db.FindTable(name)
	if err == nil {
		return TableViewer{t.Engine().(*pt.Table)}
	}
	if idx, err := db.FindIndex(name); err == nil {
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
	switch extra {
	case "journal":
		desc.PackId = -1
		desc.HavePackId = true
	// case "tomb":
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
	t.SetTitle("Schema %s [0x%x] - %d fields - %d bytes", s.Name, s.Hash, s.NumFields(), s.MinWireSize)
	t.AppendHeader(table.Row{"#", "Name", "Type", "Flags", "Filter", "Size", "Compress"})
	for _, f := range s.Fields {
		var filter string
		if f.Filter > 0 {
			filter = f.Filter.String()
		}
		t.AppendRow([]any{
			f.Id,
			f.Name,
			f.TypeName(),
			f.Flags.String(),
			filter,
			f.WireSize(),
			f.Compress,
		})
	}
	t.Render()
}

func PrintMetadata(view StatsViewer, desc TableDescriptor, w io.Writer) {
	s := view.Schema()
	rx, _ := s.IndexId(itypes.MetaRid)
	t := table.NewWriter()
	t.SetPageSize(headRepeat)
	t.SetOutputMirror(w)
	t.SetTitle("%s - %d fields - #%016x", s.Name, s.NumFields(), s.Hash)
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
	fields := s.Fields
	t.SetOutputMirror(w)
	t.AppendHeader(table.Row{"#", "Name", "Type", "Min", "Max", "Size", "Byte/Val", "Encoder Info"})
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
			s.Name,
			md.Key,
			md.Version,
			util.PrettyInt(int(md.NValues)),
			util.ByteSize(md.DiskSize),
		)
		t.SetColumnConfigs([]table.ColumnConfig{
			{Name: "Byte/Val", Align: text.AlignRight},
		})
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
				printValue(fields[i], md.Min(i)),
				printValue(fields[i], md.Max(i)),
				sz,
				strconv.FormatFloat(float64(sz)/float64(md.NValues), 'f', 4, 64),
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

func printValue(f *schema.Field, val any) any {
	switch f.Type {
	case schema.Bytes:
		return LimitStringEllipsis(fmt.Sprintf("%x", val), 33)
	case schema.Uint16:
		if f.IsEnum() && f.Enum != nil {
			enum, ok := f.Enum.Value(val.(uint16))
			if ok {
				return enum
			}
		}
		return val
	case schema.Timestamp, schema.Date, schema.Time:
		return schema.TimeScale(f.Scale).Format(val.(time.Time))
	case schema.Int128, schema.Int256, schema.Decimal128, schema.Decimal256:
		return LimitStringEllipsis(val.(fmt.Stringer).String(), 33)
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
	for _, field := range s.Fields {
		switch field.Type {
		case schema.Bytes:
			cfgs = append(cfgs, table.ColumnConfig{
				Name: field.Name,
				Transformer: func(val any) string {
					return hex.EncodeToString(val.([]byte))
				},
			})
		case schema.Uint16:
			if field.IsEnum() && field.Enum != nil {
				cfgs = append(cfgs, table.ColumnConfig{
					Name: field.Name,
					Transformer: func(val any) string {
						enum, ok := field.Enum.Value(val.(uint16))
						if ok {
							return enum
						}
						return strconv.Itoa(int(val.(uint16)))
					},
				})
			}
		case schema.Timestamp, schema.Date, schema.Time:
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
		tomb := view.ViewTomb(desc.PackId)
		rx, _ := s.IndexId(itypes.MetaRid)
		head := make(table.Row, 0, s.NumFields()+1)
		for _, v := range append([]string{"DEL"}, s.Names()...) {
			head = append(head, any(v))
		}
		t.AppendHeader(head)
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
	head := make(table.Row, 0, s.NumFields()+1)
	for _, v := range s.Names() {
		head = append(head, any(v))
	}
	t.AppendHeader(head)
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

func LimitStringEllipsis(s string, l int) string {
	c := utf8.RuneCountInString(s)
	if c <= l {
		return s
	}

	c = 0
	var b bytes.Buffer
	for _, runeVal := range s {
		b.WriteRune(runeVal)
		c += 1
		if c >= l-3 {
			break
		}
	}

	return b.String() + "..."
}
