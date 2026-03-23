# DuckDB + LakeSearch Arrow Flight Demo

Demonstrates how the same Parquet files serve both columnar analytics
(DuckDB direct scan) and full-text search (LakeSearch via Arrow Flight).

## Prerequisites

- Rust toolchain (to build LakeSearch)
- Python 3.10+ with `pyarrow` and `duckdb`

## Setup

All commands run from the **repo root** (`lakesearch/`).

### 1. Generate test data

```bash
uvx --with pyarrow python demo/generate_data.py
```

This creates `demo/data/events.parquet` with ~10K rows of synthetic log events.

### 2. Create the LakeSearch table and index

```bash
# Create table
cargo run -p lakesearch-cli -- create-table \
    --location "file://$(pwd)/demo/data/lakesearch/" \
    --table-name events \
    --column description

# Index the parquet file (path is relative to the file:// store root)
cargo run -p lakesearch-cli -- index \
    --location "file://$(pwd)/demo/data/lakesearch/" \
    --file "$(pwd)/demo/data/events.parquet" \
    --column description
```

### 3. Generate server config and start the query server

```bash
# Generate config.yaml with absolute paths
sed "s|WORKDIR|$(pwd)|g" demo/config.yaml > demo/config_local.yaml

# Start the server (runs on :8080 REST + :8081 Flight)
cargo run -p lakesearch-query -- --config demo/config_local.yaml
```

### 4. Run the demo (in another terminal)

```bash
uvx --with pyarrow --with duckdb python demo/demo.py
```

## What the demo shows

| Query | Method | What it demonstrates |
|-------|--------|---------------------|
| 1 | DuckDB scans Parquet directly | Pure OLAP: p99 latency by service |
| 2 | Flight `do_get` → DuckDB | Full-text search for "connection timeout", aggregated by service |
| 3 | Flight `do_get` → DuckDB | Broader "error" search with aggregation |
| 4 | Flight results joined with Parquet scan | Text search as a dimension table joined with analytics |

## Files

```
demo/
├── README.md          — This file
├── config.yaml        — Server config template (WORKDIR placeholder)
├── generate_data.py   — Creates events.parquet
├── demo.py            — The DuckDB + Flight demo script
└── data/              — Generated (gitignored)
    ├── events.parquet
    └── lakesearch/    — Table metadata + index segments
```
