// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

// Package schema implements generic type system management and encoding
// for open data exchange in data processing pipelines. It is intended to
// serve as foundation for schema registries, databases, query engines
// and stream processors.
//
// Core design goals
//
// - expressive and composable data types
// - fast encoding/decoding data into binary records
// - decode-free direct data access to binary records
// - schema evolution and versioning
//
// A schema is a list of immutable fields with properties like name, data type,
// and type specific options (decimal scale, fixed length). Each field is uniquely
// identified by an immutable id value. Type and id may not change, but schema
// evolution is possible in several ways:
//
// - the name of a field may be changed
// - a new field may be added
// - an existing fields can be marked as deleted
//
// Every change produces a new schema version which is identified by a
// sequential version number and a unique hash value. Schemas can either
// be created programmatically `schema.SchemaOf` or be infered from Go types
// and struct tags `reflect.SchemaOf`.
//
// Schemas are flat, single-level field type lists. There is no concept
// of structs, however users can use dot separator in field names to simulate
// such structure. Schemas may, however, be nested using list and map types
// which allow repetition of nested values. Nesting still produces a flat
// schema type (all fields are in a single field list), but relations and
// nesting level is explicit. Writer and accessor APIs offer special methods
// for working with nested values.
//
// Supported field types
//
// - Timestamp (UTC timestamp with configurable resolution s..ns)
// - Duration (time duration with configurable resolution s..ns)
// - Date (calendar date)
// - Time (time of day with configurable resolution s..ns)
// - Uint64
// - Uint32
// - Uint16
// - Uint8
// - Int64
// - Int32
// - Int16
// - Int8
// - Boolean
// - Float64
// - Float32
// - Int256
// - Int128
// - Decimal256 (fixed-point int256 with scale 0..76)
// - Decimal128 (fixed-point int128 with scale 0..38)
// - Decimal64 (fixed-point int64 with scale 0..18)
// - Decimal32 (fixed-point int32 with scale 0..9)
// - Bigint (bigint with max 255 byte precision)
// - String (short fixed or variable size string <= 255 bytes)
// - Text (UTF8 text up to 4GB)
// - Bytes (short fixed or variable size binary string <= 255 bytes)
// - Binary (a large binary value)
// - List (a list of primitive types, structs or nested types)
// - Map (a list of key/value pairs)
// - Union (a type wrapper for any supported type excect Union itself)
//
// Flags control field properties
//
// - primary: the field is used as primary key (must be uint64 type)
// - timebase: timestamp field used as event time source for stream processing
// - enum: the field is an enum type with an attached EnumDictionary
// - array: a fixed length string or byte field
// - nullable: values can be null
// - deleted: the field is deleted and no longer used for encoding
// - metadata: additional metadata field excluded from binary format
// - action: CDC action metadata field
//
// Defining schemas with Go struct tags
//
// ```
// Flags
// -----
// primary       mark this field as primary key (also generates primary key index)
// enum          mark as enum
// metadata      mark as metadata
// null          mark as nullable
// notnull       mark as not nullable
// timebase      mark as event time source
//
// Type overrides
// --------------
// array={num}   fixed length string (use type [n]byte for byte array)
// timestamp     timestamp in configured resolution (see scale)
// date          date in unix days
// time          time of day in configured resolution (see scale)
// text          UTF8 text up to 2^32-1 bytes long
// binary        binary data up to 2^32-1 bytes long
// element       tags for list items (e.g. name,notnull,element=date)
// key,value     tags for map items (e.g. name,notnull,key=date,value=scale=4)
//
// Options
// -------
// filter={type} use db column filter (bits, bloom2..5b, bfuse8/16)
// zip={type}    use extra compression (snappy, lz4, zstd, none, (empty))
// scale={num}   scale factor for decimal [0..9,18,38,76] and time [ns,us,ms,s,d]
// id={num}      override id value
//
// Indexes
// -------
// index={type}  generate index over this field (hash, int, composite)
// fields={a+b}  list of composite index fields
// extra={a+b}   list of extra include fields for index
// ```
//
// Scale factors
// -------------
//
// Time scales
// - `ns` store time in nanosecond resolution
// - `us` stores time in microsecond resolution
// - `ms` stores time in millisecond resolution
// - `s` stores time in second resolution
// - `d` stores time in days resolution
//
// Decimal (fixed-point scales)
// - `Decimal32` values from `0..9`
// - `Decimal64` values from `0..18`
// - `Decimal128` values from `0..38`
// - `Decimal256` values from `0..76`
//
// Schema Evolution
// ----------------
//
// Schemas are explicitly versioned with a monotonic version number and uniquely
// identified by a hash value. Every schema change (add/rename/delete field)
// increments the version of a schema and all its parent schemas and changes the
// schema hash. The schema version is stored on record headers
// and column store metadata. Encoders never write deleted fields, decoders
// treat deleted fields as if they don't exist. When new decoders read records
// produced by old schema versions, newly added fields default to nil (when
// nullable) or a type domain neutral value (zero).
//
// - a schema may contain at most 2^16-1 fields
// - fields can be added, deleted and renamed
// - changing a field's type or traits is forbidden
// - field ids are immutable and unique, id reuse is forbidden
// - adding a field at any nesting level assigns a new id
// - re-adding a deleted field assigns a new id
// - deleted fields are marked as deleted and remain in the schema forever
// - deleting a field with nested children marks all children as deleted
//
// variant specific rules
// - a variant may contain at most 255 cases
// - fields in cases can be added, deleted and renamed
// - cases can be added and deleted
// - case id reuse is forbidden
// - re-adding a deleted case assigns a new case id
//
// reader rules
// - (todo) forward/backward compatibility: read/skip/zero/fail
//
// Migration
