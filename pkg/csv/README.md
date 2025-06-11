# Go CSV Encoder

A [Go](http://golang.org/) package for encoding and decoding CSV-structured textfiles to/from Knox schema using static Go types or dynamic reflect struct types. The main purpose of this package is the import and export of CSV data into KnoxDB tables. As such it is based on KnoxDB schema definitions and handles only supported data types.

The code is separated into the following models
- `Sniffer` detects CSV features and data types, can produce dynamic type schema
- `Reader` reads CSV streams, splits lines and fields
- `Decoder` decodes CSV records into static/dynamic Go structs
- `Encoder` writes CSV streams from static/dynamic Go structs

An important feature of encoders and decoders is that they work with dynamic Go types (i.e. structs that do not have a static struct definition in a Go program). This allows to work with arbitrary CVS files and user defined schemas. Internally the decoder uses `reflect.MakeStruct` and `reflect.New` to produce a dynamic Go type. One limitation of this type is that it does not have a type name (although fields have correct names) and it cannot be used as Go generic (it does not exist at compile time).

## Features

- [RFC 4180](https://tools.ietf.org/rfc/rfc4180.txt) compliant and robust to broken quotes
- support for all KnoxDB data types (strings, integers, floats, boolean, decimals, time)
- supports bulk and stream processing
- custom separator, comment, eol characters
- optional whitespace trimming for headers and string values
- detects timestamp format, allows custom timestamp formats

Supported data types
- int (8, 16, 32, 64, 128, 256 bit and bigint) signd and unsigned
- float (32, 64) bit
- decimal (32, 64, 128, 256 bit)
- strings
- byte blobs and fixed length byte arrays (hex encoded, optionally 0x prefixed)

Sniffer detects
- separator character
- number of fields
- time format for timestamp fields
- if a header is present
- if text fields contain trimmable whitespace
- if the file uses quotes and escaped quotes
- if null fields are present
- data type and names of fields


## Limitations

- no support for Go types `int`, `uint`, `complex`
- no custom marshaler interfaces (`BinaryMarshaler`, `TextMarshaler`)

## Examples

### Detecting CSV schema

Use Sniffer to detect structure and field types of a CVS file. Sniffer analyzes text lines, determines types and type features, and creates a KnoxDB compatible schema.

```go
import "blockwatch.cc/knoxdb/pkg/csv"

func DetectFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// sniff 100 random lines (including header, leading comments, first line)
	sniff := csv.NewSniffer(f, 100)
	if err := sniff.Sniff(); err != nil {
		return err
	}

	// obtain sniff result and KnoxDB schema
	res := sniff.Result()
	s := sniff.Schema()

	// rewind f and use sniffer to create and configure a decoder
	f.Seek(0, 0)
	dec := sniff.NewDecoder(f)

	// use decoder ...
}

```

### Reading with a pre-defined static type

Although uncommon for KnoxDB use cases it is possible to decode CSV with a static Go type through `GenericDecoder`. However you need to define a matching pair of logical and physical types. The CSV decoder uses native (physical) storage layout, hence returned values are of the physical type. The logical type is required to define features for custom KnoxDB types like time scale and decimal scale.

```go
import "blockwatch.cc/knoxdb/pkg/csv"

type LogType struct {
	Id     int64         `knox:"id,pk"`
	Time   time.Time     `knox:"time,scale=ms"`
	Amount num.Decimal64 `knox:"amount,scale=5"`
}

type PhyType struct {
	Id     int64 `csv:"id"`
	Time   int64 `csv:"time"`
	Amount int64 `csv:"amount"`
}

func ReadFile(path string) ([]*PhyType, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

    dec := csv.NewGenericDecoder[LogType, PhyType](f)
	res := make([]*PhyType, 0)
	for {
		val, err := dec.Decode()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		res = append(res, val)
	}
	return res, nil
}
```

