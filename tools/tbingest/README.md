# tbingest

A simple command-line tool for ingesting TigerBeetle-style `Account` and `Transfer` data into KnoxDB packfiles.

This tool reads a JSON array of records and converts them into KnoxDB `.knox` format using the v2 columnar container system.

---

## Installation

No install needed. Just run via Go:

```bash
go run ./tools/tb_ingest/main.go --mode=account --input=accounts.json --output=accounts.knox
```

---

## Input File Format

The input must be a `.json` file containing an array of objects.

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
# Ingest transfer records into a KnoxDB file
$ go run ./tools/tb_ingest/main.go \
    --mode=transfer \
    --input=transfers.json \
    --output=transfers.knox

# Ingest account records
$ go run ./tools/tb_ingest/main.go \
    --mode=account \
    --input=accounts.json \
    --output=accounts.knox
```

---

## Notes

* `id` fields must be given as `[high, low]` 64-bit pairs to represent u128 values.
* The tool uses the KnoxDB `stream.Writer` API and appends rows in packfile format.

---

## Output

The output will be a compressed `.knox` file compatible with KnoxDB v2 containers:

* `AccountContainer`
* `TransferContainer`

These can be scanned or queried using standard KnoxDB tooling.

---

## Design Philosophy

This tool acts as a bridge between TigerBeetle-style operational systems and KnoxDB's analytical engine, allowing efficient ingestion of double-entry financial data for compression, querying, and archival.