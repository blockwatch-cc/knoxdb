package main

import (
	"bytes"
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

var (
	Package string = "bitpack"
)

type Data struct {
	Package string
	Bits    int
}

func main() {

	if err := run(); err != nil {
		log.Fatal("err", err)
	}
}

func run() error {
	bitsData := []Data{
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
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	cwd = filepath.Join(cwd, "internal", "encode", "bitpack")

	t, err := loadTemplate(filepath.Join(cwd, "generate", "generic.go.tmpl"))
	if err != nil {
		return err
	}

	for _, bitData := range bitsData {
		buffer := new(bytes.Buffer)
		err := t.Execute(buffer, bitData)
		if err != nil {
			return err
		}

		res, err := format.Source(buffer.Bytes())
		if err != nil {
			return err
		}

		fname := fmt.Sprintf("uint%d.go", bitData.Bits)
		os.WriteFile(filepath.Join(cwd, fname), res, PERM)
	}
	return nil
}

func loadTemplate(fname string) (*template.Template, error) {
	funcMap := template.FuncMap{
		"inc":           inc,
		"dec":           dec,
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
