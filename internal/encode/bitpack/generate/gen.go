package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"log"
	"math"
	"os"
	"path/filepath"
	"text/template"
)

const (
	PERM = 0644
)

type Data struct {
	Package string
	Bits    int
}

type CompareData struct {
	Package  string
	OpName   string
	Op       string
	Template string
}

type PackData struct {
	Templ         string
	BitsData      []Data
	OutFilePrefix string
}

var (
	Package string = "bitpack"

	opsData = []CompareData{
		{
			Package:  Package,
			OpName:   "eq",
			Op:       "==",
			Template: "cmp.go.tmpl",
		},
		{
			Package:  Package,
			OpName:   "lt",
			Op:       "<",
			Template: "cmp.go.tmpl",
		},
		{
			Package:  Package,
			OpName:   "le",
			Op:       "<=",
			Template: "cmp.go.tmpl",
		},
		{
			Package:  Package,
			OpName:   "bw",
			Op:       "<=",
			Template: "cmp_bw.go.tmpl",
		},
	}

	packData = []PackData{
		{
			Templ: "pack.go.tmpl",
			BitsData: []Data{
				{
					Package: Package,
					Bits:    8,
				},
				{
					Package: Package,
					Bits:    16,
				},
				{
					Package: Package,
					Bits:    32,
				},
				{
					Package: Package,
					Bits:    64,
				},
			},
			OutFilePrefix: "uint",
		},
		{
			Templ: "float.pack.go.tmpl",
			BitsData: []Data{
				{
					Package: Package,
					Bits:    32,
				},
				{
					Package: Package,
					Bits:    64,
				},
			},
			OutFilePrefix: "float",
		},
	}

	op string
)

func main() {
	flag.StringVar(&op, "op", "pack", "operation")
	flag.Parse()

	if err := run(); err != nil {
		log.Fatal("err", err)
	}
}

func run() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	cwd = filepath.Join(cwd, "internal", "encode", "bitpack")

	switch op {
	case "pack":
		return pack(cwd)
	case "cmp":
		return compare(cwd)
	}
	return nil
}

func pack(cwd string) error {

	for _, pd := range packData {
		t, err := loadTemplate(filepath.Join(cwd, "generate", pd.Templ))
		if err != nil {
			return err
		}

		for _, bitData := range pd.BitsData {
			buffer := new(bytes.Buffer)
			err := t.Execute(buffer, bitData)
			if err != nil {
				return err
			}

			res, err := format.Source(buffer.Bytes())
			if err != nil {
				return err
			}

			fname := fmt.Sprintf("%s%d.go", pd.OutFilePrefix, bitData.Bits)
			os.WriteFile(filepath.Join(cwd, fname), res, PERM)
		}
	}

	return nil
}

func compare(cwd string) error {
	for _, opData := range opsData {
		t, err := loadTemplate(filepath.Join(cwd, "generate", opData.Template))
		if err != nil {
			return err
		}
		buffer := new(bytes.Buffer)
		err = t.Execute(buffer, opData)
		if err != nil {
			return err
		}
		res, err := format.Source(buffer.Bytes())
		if err != nil {
			return err
		}
		fname := fmt.Sprintf("cmp_%s.go", opData.OpName)
		os.WriteFile(filepath.Join(cwd, fname), res, PERM)
	}
	return nil
}

func loadTemplate(fname string) (*template.Template, error) {
	funcMap := template.FuncMap{
		"inc":           inc,
		"dec":           dec,
		"mod":           mod,
		"bitsFuncRange": bitsFuncRange,
		"bitRange":      bitRange,
	}
	f, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	return template.New("rangeTemplate").Funcs(funcMap).Parse(string(f))
}

func inc(v, y int) int {
	return v + y
}

func dec(v, y int) int {
	return v - y
}

func mod(x, y int) int {
	return x % y
}

func bitsFuncRange(bits int) []int {
	return make([]int, bits)
}

func bitRange(bitsize, bitIndex, offset int) []int {
	v := int(math.Ceil(float64(bitsize) / float64(bitIndex)))
	if offset > 0 {
		v++
	}
	return make([]int, v)
}
