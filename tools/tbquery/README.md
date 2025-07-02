# tbquery

CLI tool for inspecting and debugging `.knox` container files that encode TigerBeetle `AccountContainer` and `Transfer` data inside KnoxDB packfiles.

Built for fast verification, raw inspection, and simple filtering.

---

## Features

- Reads `.knox` files with `AccountContainer` or `Transfer` rows
- Pretty-prints decoded rows as JSON (default)
- Supports filtering by `id` field
- Can output raw JSON (one row per line)
- Compatible with KnoxDB v2 container schema

---

## Usage

```bash
go run ./tools/tbquery/main.go <file.knox> [--raw] [--id=low:high]
````

---

### Examples

#### Decode all records (pretty-print)

```bash
go run ./tools/tbquery/main.go transfers.knox
```

#### Filter by specific ID

```bash
go run ./tools/tbquery/main.go transfers.knox --id=123:456
```

#### Raw JSON output (newline-delimited)

```bash
go run ./tools/tbquery/main.go transfers.knox --raw
```

---

## Notes

* ID filtering applies to 128-bit IDs (Lo\:Hi)
* Automatically detects container type (Transfer or Account)
* Useful for testing ingestion outputs and verifying semantics

---

## Notes

Uses KnoxDB's internal `pack` encoder and `jsonutil` for formatting.

Expect to extend this tool with:

* Range filters (e.g., by timestamp or amount)
* Flag visualization or filtering
* Columnar query interface (future integration)