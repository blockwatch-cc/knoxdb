# internal/containers

This package defines the schema, encoding, and logic for KnoxDB's typed columnar containers. Containers are the primary data structure for storing and querying compressed structured data.

---

## Purpose

Each container type defines:

* The Go struct representing a row
* The field encodings via `knox:"..."` struct tags
* Associated metadata for compression, serialization, and analysis

These containers are used in:

* Ingestion pipelines
* Stream writers/readers
* Query plans and execution

---

## Key Files

### `account_container.go`

Implements the `AccountContainer`, mapping the TigerBeetle account structure into a compressed, append-only format.

Encodes fields such as:

* `id`: `[2]uint64` (u128), bitpacked
* `debits_posted`, `credits_posted`: compressed using ALP-RD
* `ledger`, `code`, `flags`: categorical/dictionary or raw encodings

### `transfer_container.go`

Implements the `TransferContainer`, including:

* `id`, `debit_account_id`, `credit_account_id`: all `[2]uint64`, bitpacked
* `amount`: ALP-RD
* `pending_id`: dictionary (nullable or repeated)
* `flags`, `timestamp`: raw or delta encoding

### `transfer_semantics.go`

Defines TigerBeetle-style validation and invariants for transfers:

* Ensures correct pending/post/void states
* Guards against flag collision and invalid transitions
* Used in pre-ingestion validation or offline audits

### `*_test.go`

Test files for roundtrip encoding/decoding:

* Ensure semantic integrity
* Verify byte-for-byte serialization
* Assert compatibility with stream writer/reader logic

---

## Design Principles

* **Immutable rows**: Containers are append-only
* **Columnar encodings**: Per-field codec optimizations (ALP-RD, delta, bitpacked)
* **Strong semantics**: Invariant checks ensure transaction integrity
* **Modularity**: Easy to register new container types via `RegisterAll()`

---

## Testing Strategy

Each container has a `*_test.go` that:

* Generates synthetic data
* Encodes into a Knox pack
* Decodes back and verifies structural + semantic correctness

Use `go test ./internal/containers/...` to run all tests.

---

## Adding a New Container

1. Define a new struct in `pkg/internal/containers/`
2. Use `knox:"name,encoding=..."` tags to define column encoding
3. Register it in `RegisterAll()`
4. Add tests for encoding/decoding and invariant validation