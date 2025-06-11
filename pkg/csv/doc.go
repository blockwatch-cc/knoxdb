// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package csv

// Package csv decodes and encodes comma-separated values (CSV) files to and from
// arbitrary Go types. Because there are many different kinds of CSV files, this
// package implements the format described in RFC 4180.
//
// A CSV file may contain an optional header and zero or more records of one or
// more fields per record. The number of fields must be the same for each record
// and the optional header. The field separator is configurable and defaults to
// comma ',' (0x2C). Empty lines and lines starting with a comment character
// are ignored. The comment character is configurable as well and defaults to
// the number sign '#' (0x23). Records are separated by the newline character
// '\n' (0x0A) and the final record may or may not be followed by a newline.
// Carriage returns '\r' (0x0D) before newline characters are silently removed.
//
// White space is considered part of a field. Leading or trailing whitespace
// can optionally be trimmed when parsing a value. Fields may optionally be quoted
// in which case the surrounding double quotes '"' (0x22) are removed before
// processing. Inside a quoted field a double quote may be escaped by a preceeding
// second double quote which will be removed during parsing.
