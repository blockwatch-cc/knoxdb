// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

// Package schema defines type system management for database tables with
// two main purposes
//
// - defining structure and configuration for database tables
// - encoding/decoding binary records and accessing data in binary records without decoding
//
// Schemas can either be created programmatically from types `Schema` and `Field` or
// by adding struct tag `knox` to a user-defined struct and then calling `SchemaOf`.
// The following struct tag features are available
//
// ```
// pk            mark this field as primary key
// index={type}  generate db index (hash, int, bits, bloom)
// zip={type}    use extra compression (snappy, lz4, zstd, none, (empty))
// fixed={num}   treat as fixed length field (only byte array, byte slice, string)
// scale={num}   use fixed point scale factor (decimal types only)
// enum          mark field as enum
// internal      mark field as internal (not used in encode/decode)
// id={num}      override id value
// ```
//
// A schema is a list of immutable fields with properties like name, data type,
// and type specific options (decimal scale, fixed length). Each field is unique and
// identified by an immutable id value. Type and id may not change, but schema
// evolution is possible in several ways:
//
// - the name of a field may be changed
// - a new field may be added
// - an existing fields being marked as deleted
//
// Each change produces a new version of the schema which is identified by a
// unique hash value.
//
// Internally, field flags are used to represent properties such as
// - primary key: the field is used as primary key (must be uint64 type)
// - indexed: a database index will be created for this field
// - enum: the field is an enum type with a private EnumDictionary
// - deleted: the field is deleted and no longer used
// - internal: the field is not used for encoding and decoding data
//
// Flags define how fields are used by record encoders and decoders:
// - `visible` means a field is used when encoding/decoding binary records from Go structs;
//    a visible field is never deleted or internal
// - `active` means the field (internal or not) is in active use, i.e. it is not deleted
// - `internal` means all non deleted internal fields
