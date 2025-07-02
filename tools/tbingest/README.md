# tbingest

A minimal CLI tool for ingesting TigerBeetle-style `Account` and `Transfer` data into KnoxDB `.knox` packfiles using the v2 columnar container format.

---

## Features

- Supports **JSON** and **Parquet** input with 128-bit ID fields
- Ingests both `Account` and `Transfer` records
- Validates **transfer semantics** (e.g. flag consistency, unique IDs, pending references)
- Outputs `.knox` packfiles readable by `tbquery` or KnoxDB scanners
- Built for seamless integration with financial systems and archival analytics

---

## Installation

No install needed — run directly:

```bash
go run ./tools/tbingest/main.go --mode=account --input=accounts.json --output=accounts.knox
````

---

## Input File Formats

Input must be a `.json` or `.parquet` file containing an array of records.

### Example: `accounts.json`

```json
[
  {
    "id": [123, 456],
    "debits_pending": 0,
    "credits_pending": 0,
    "debits_posted": 1000,
    "credits_posted": 2000,
    "ledger": 1,
    "code": 42,
    "flags": 0
  }
]
```

### Example: `transfers.json`

```json
[
  {
    "id": [111, 222],
    "debit_account_id": [123, 456],
    "credit_account_id": [789, 101112],
    "amount": 500,
    "pending_id": [0, 0],
    "ledger": 1,
    "code": 42,
    "flags": 1,
    "timestamp": 1720000000
  }
]
```

---

## Usage

```bash
# Ingest transfers from JSON
go run ./tools/tbingest/main.go \
    --mode=transfer \
    --input=transfers.json \
    --output=transfers.knox

# Ingest accounts from Parquet
go run ./tools/tbingest/main.go \
    --mode=account \
    --input=accounts.parquet \
    --output=accounts.knox
```

Supported extensions: `.json` and `.parquet`

---

## Transfer Validation

All transfers undergo strict semantic checks:

* No duplicate IDs
* Valid flag combinations (`post`/`void` vs `pending`)
* Referenced `pending_id`s must exist
* `id` must be non-zero

Failing any of these checks aborts ingestion with a clear error message.

---

## Output

Creates a compressed `.knox` file containing:

* `TransferContainer` rows for `transfer` mode
* `AccountContainer` rows for `account` mode

These are readable by `tbquery` and other KnoxDB tooling.

---

## Notes

* `id` and `pending_id` must be 128-bit `[uint64, uint64]` arrays
* Uses the `stream.Writer` API from the v2 KnoxDB columnar pack system
* Input detection is automatic based on file extension