// Copyright (c) 2024-2026 Blockwatch Data Inc.
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
// Flags
// ------
// primary       mark this field as primary key (also generates primary key index)
// enum          mark as enum
// metadata      mark as metadata
// null          mark as nullable
// timebase      mark as event time source
//
// Types
// ------
// array={num}   fixed length string (use type [n]byte for byte array)
// timestamp     timestamp in nanoseconds
// date          date in unix days
// time          time in seconds
// text          UTF8 text up to 2^32-1 bytes
// blob          binary data up to 2^32-1 bytes
// element       tags for list items (e.g. name,notnull,element=date)
// key,value     tags for map items (e.g. name,notnull,key=date,value=scale=4)
//
// Options
// --------
// filter={type} use db column filter (bits, bloom2..5b, bfuse8/16)
// zip={type}    use extra compression (snappy, lz4, zstd, none, (empty))
// scale={num}   scale factor for decimal [0..9,18,38,76] and time [ns,us,ms,s,d]
// id={num}      override id value
//
// Indexes
// --------
// index={type}  generate index over this field (hash, int, composite)
// fields={a+b}  list of composite index fields
// extra={a+b}   list of extra include fields for index
// ```
//
// A schema is a list of immutable fields with properties like name, data type,
// and type specific options (decimal scale, fixed length). Each field is unique and
// identified by an immutable id value. Type and id may not change, but schema
// evolution is possible in several ways:
//
// - the name of a field may be changed
// - a new field may be added
// - an existing fields can be marked as deleted
//
// Each change produces a new version of the schema which is identified by a
// unique hash value.
//
// Internally, field flags are used to represent properties such as
// - primary key: the field is used as primary key (must be uint64 type)
// - indexed: a database index will be created for this field
// - enum: the field is an enum type with a private EnumDictionary
// - deleted: the field is deleted and no longer used
// - metadata: the field is not used for encoding and decoding data
// - nullable: values can be null
// - timebase: timestamp field used as event time source for stream processing
// - action: CDC action metadata field
//
// Flags define how fields are used by record encoders and decoders:
// - `visible` means a field is used when encoding/decoding binary records from Go structs;
//    a visible field is never deleted or internal
// - `active` means the field (internal or not) is in active use, i.e. it is not deleted
// - `metadata` means all non deleted internal fields
//
// Scale factor
//
// Time scales
// - `ns` store time in nanosecond resolution
// - `us` stores time in microsecond resolution
// - `ms` stores time in millisecond resolution
// - `s` stores time in second resolution
//
// Decimal (fixed-point scales)
// - `Decimal32` values from `0..9`
// - `Decimal64` values from `0..18`
// - `Decimal128` values from `0..38`
// - `Decimal256` values from `0..76`
