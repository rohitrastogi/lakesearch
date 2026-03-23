# LakeSearch v2 Design: Metadata, Ingest, and Compaction

## Overview

This document captures design decisions from reviewing the metadata
protocol, query path, ingest pipeline, and compaction strategy. It
describes the target architecture and the problems it solves.

---

## 1. Current Problems

### 1a. Redundant metadata layers
4 levels of indirection: `current.json → metadata → manifest lists →
manifests → segments`. The manifest layer always has 1 segment — pure
overhead adding an I/O hop to every query.

### 1b. No explicit file registry
Data file awareness is implicit — the indexer receives file paths in
task payloads and records them in manifest lists. No single source of
truth for "what files exist for this table."

### 1c. Query loads everything
Every query loads all manifest lists to compute `all_files -
indexed_files` for brute-force scanning. In steady state (everything
indexed), this produces an empty set — wasted I/O.

### 1d. No segment pruning in practice
The query planner prunes segments by term range, but every segment
covers the full vocabulary. Nothing gets pruned.

### 1e. Compaction is all-or-nothing
Merges all segments when a count threshold is exceeded. No size
awareness, unbounded cost, no term-range optimization.

### 1f. Tightly coupled services
Separate admin, indexer (cascadq-driven), and compactor services with
inter-service coordination. More complexity than needed.

---

## 2. Architecture Principles

### Query and maintenance are independent
- **Query** is stateless and read-only. Reads metadata + segments,
  returns results. Deployable as a server, Lambda, CLI, or embedded
  library. Can scale to zero.
- **Maintenance** is stateful and read-write. Registers files, indexes,
  compacts, GCs. Deployable as a daemon, cron job, CLI command, or
  on-demand invocation.

They share nothing at runtime. The metadata protocol in object storage
(coordinated by CAS) is the only interface between them. They don't
need to be co-located, co-deployed, or even running at the same time.

### The user controls composition
LakeSearch provides libraries and optional binaries. The user decides
how to deploy:
- Register files via CLI, index via cron, query via Lambda
- Or run everything in one server process
- Or embed the query library in their application

### Metadata protocol is the API boundary
All coordination happens through the metadata files in object storage.
No task queues, no service-to-service RPCs, no shared databases.

---

## 3. File Registry

### Concept
Data file awareness is an explicit, first-class operation. Metadata
maintains a registry of known data files for the table.

**LakeSearch never lists or discovers files from object storage.**
The only way a data file enters the system is through explicit
registration by the user. This is a hard constraint — no directory
listing, no prefix scanning, no implicit discovery.

### Operations
- **Register files** (additive) — the user explicitly adds parquet
  file paths to the registry. This is the sole entry point for data
  files. Called via CLI (`lakesearch register`), HTTP API
  (`POST /v1/tables/{table}/files`), or library function
  (`register_files()`). The caller provides the file paths —
  LakeSearch does not discover them. New files are appended to the
  existing registry.
- **Replace files** (destructive) — the user provides the complete,
  current set of data files. LakeSearch diffs against the existing
  registry: files not in the new set are removed, files not in the
  old set are added. Segments that reference any removed file are
  dropped (their manifest list entries are removed from the snapshot).
  New files are queued for indexing. Called via CLI
  (`lakesearch replace-files`), HTTP API
  (`PUT /v1/tables/{table}/files`), or library function
  (`replace_files()`). This is the mechanism for handling upstream
  data compaction — when a user compacts their Parquet files, they
  call replace with the new file list, and LakeSearch invalidates
  stale segments and reindexes.
- **Index** — runs automatically in the maintenance loop. Picks up
  registered-but-not-indexed files and creates segments. Can also
  be invoked directly for synchronous behavior.

Registering and indexing are decoupled. The user registers files;
the system indexes them in the background.

### Upstream data compaction
When users compact their Parquet files (outside LakeSearch), the
files our segments point to may be deleted or replaced. Our segments
become invalid — they reference row groups in files that no longer
exist.

LakeSearch does not detect this automatically. The user must tell us
by calling **replace-files** with the new complete file list after
compaction. This:
1. Removes deleted files from the registry
2. Drops all segments that referenced any removed file
3. Registers the new compacted files
4. Clears `fully_indexed` — triggers reindexing of the new files

This is a potentially expensive operation (full reindex), but it's
correct. The alternative — silently serving results from segments
pointing at deleted files — is worse.

### Metadata representation
`data_files` moves from per-manifest-list to table-level in metadata.
This is the authoritative registry — the single source of truth for
what data files belong to this table:
```json
{
  "data_files": [
    {"path": "s3://.../part-001.parquet", "size_bytes": 134217728,
     "row_count": 100000}
  ]
}
```

Registration updates metadata via CAS: read current metadata, append
new files to `data_files`, clear `fully_indexed` on affected columns,
write new metadata snapshot.

### Fully-indexed optimization
Each indexed column tracks `fully_indexed: bool`. Set to `true` when
all registered files are indexed for that column. Cleared when new
files are registered. The query planner checks this flag — if true,
skip the unindexed-file computation entirely (no brute-force path).

---

## 4. Simplified Metadata Protocol

### Target structure (3 levels, down from 4)

```
current.json → metadata.json → manifest-list files
                                  └── segments (inline)
```

**current.json** — mutable CAS pointer.

**metadata.json** — immutable per snapshot:
```json
{
  "format_version": 2,
  "table_id": "...",
  "table_name": "events",
  "location": "s3://bucket/warehouse/events/",
  "indexed_columns": [
    {"name": "description", "tokenizer": "default",
     "status": "active", "fully_indexed": true}
  ],
  "data_files": [
    {"path": "s3://.../part-001.parquet", "size_bytes": 134217728,
     "row_count": 100000}
  ],
  "snapshot": {
    "timestamp_ms": 1711100000000,
    "manifest_lists": ["manifest-lists/ml-1.json"],
    "batch_ids": ["sha256:abc", "sha256:def"]
  }
}
```

**manifest-list file** — immutable, one per ingest/compact job:
```json
{
  "job_kind": "append",
  "batch_id": "sha256:abc",
  "segments": [
    {
      "indexed_column": "description",
      "segment_path": "segments/seg-xxx.seg",
      "size_bytes": 52428800,
      "doc_count": 400,
      "total_rows": 100000,
      "total_tokens": 500000,
      "parquet_files": [
        {"file_ordinal": 0, "path": "s3://.../part-001.parquet",
         "file_size_bytes": 134217728, "row_group_count": 4}
      ],
      "term_stats": {
        "min_term": "aardvark", "max_term": "zymurgy",
        "term_count": 85000
      }
    }
  ]
}
```

### What changed from v1
- **Manifest files removed.** Segment info inlined into manifest list.
- **`data_files` moved to metadata.** Table-level file registry.
- **`batch_ids` in metadata.** Dedup without loading manifest lists.
- **`fully_indexed` on column config.** Query fast-path optimization.

### Query path
1. `current.json` → metadata path
2. `metadata.json` → manifest list paths, `data_files`, column config
3. If `fully_indexed`: load manifest lists, prune by term stats, load
   segment bytes. No brute-force computation.
4. If not `fully_indexed`: also compute unindexed files from
   `data_files - union(segment.parquet_files)` for brute-force.

---

## 5. Compaction Strategy

### Size-tiered selection
Group segments by size tier (powers of `size_ratio`). Merge when a
tier has ≥ `min_merge_count` segments. Replaces the count-based
"merge all" strategy.

```
Tier 0:  0 – 1MB       (fresh ingest segments)
Tier 1:  1MB – 10MB
Tier 2:  10MB – 100MB
Tier 3:  100MB – 1GB    (target "done" size)
Tier 4:  1GB+           (never merge)
```

`size_bytes` is available on segment entries in manifest lists —
no extra I/O to plan compaction.

### Term-range splitting
When a merged segment exceeds `target_segment_size`, split by term
range into N partitions with disjoint `[min_term, max_term]`. Each
partition becomes a separate segment entry.

This enables the existing query pruning logic. A query for "zebra"
only loads segments whose term range includes "zebra".

### Compaction output
One new manifest list containing:
- N segment entries for the compacted column (1 if no split, N if split)
- Carried-forward segment entries for other columns from replaced
  manifest lists

Replaces the old manifest lists in the metadata snapshot.

---

## 6. Crate Structure

```
lakesearch-core        — types, segment format, metadata protocol,
                         storage I/O, tokenizer, BM25, posting codec
lakesearch-query       — query library (embeddable) + optional
                         HTTP/Flight server binary
lakesearch-maintain    — maintenance library (embeddable) + optional
                         daemon binary (replaces indexer, compactor,
                         admin)
lakesearch-cli         — CLI wrapping both libraries
```

Everything is library-first. The servers and CLI are thin wrappers.

- Embed `lakesearch-query` in your own query planner to get Arrow
  batches directly. Or run the included HTTP/Flight server.
- Call `lakesearch-maintain::register_files()`, `run_index()`,
  `run_compact()` from your own orchestrator. Or run the included
  daemon. Or use the CLI for one-shot operations.

Query and maintenance share nothing at runtime. Both operate on
metadata in object storage, coordinated by CAS.

### Deployment models

**Query** — run as a server or serverless:
- `lakesearch-query` binary: long-running HTTP + Flight server
- Or embed the library in a Lambda / serverless function
- Stateless, read-only, scales horizontally, can scale to zero

**Maintenance** — run as a service or one-shot:
- As a daemon: `lakesearch-maintain` binary with periodic background
  loop + optional HTTP API for registration
- As one-shot CLI commands: `lakesearch register`, `lakesearch index`,
  `lakesearch compact`, `lakesearch gc`
- Stateful (mutates metadata), single-writer per table (CAS ensures
  safety if multiple run concurrently)

All write operations (register, index, compact, GC) are library
functions in `lakesearch-maintain`. The daemon and CLI are just
different callers of the same functions. A user who only wants
one-shot operations never needs to run a server — the CLI talks
directly to object storage.

### Maintenance daemon (optional)
When run as a long-lived service:
- HTTP API for registration:
  `POST /v1/tables/{table}/files`,
  `POST /v1/tables/{table}/columns`
- Background loop for indexing, compaction, GC
- Health endpoint

### CLI one-shot (no server required)
When run as CLI commands, each does its work directly against object
storage and exits. No daemon needed.

### Maintenance daemon loop
```
loop:
  for table in tables:
    1. Check for registered-but-unindexed files → run_index
    2. Check for fragmented segments → run_compact (size-tiered + split)
    3. Check for orphaned files → gc_sweep
  sleep(poll_interval)
```

### CLI one-shot commands
```
lakesearch register      --table events --files 's3://bucket/data/*.parquet'
lakesearch replace-files --table events --files 's3://bucket/data/*.parquet'
lakesearch index         --table events --column description
lakesearch compact       --table events
lakesearch gc            --table events
lakesearch query         --table events --match "error timeout"
```

---

## 7. Implementation Order

Each step is independently useful and shippable:

1. **Drop Manifest layer** — inline SegmentInfo into manifest list
   entries. Mechanical refactor, removes one I/O hop.

2. **File registry** — move `data_files` to metadata, add registration
   API, add `fully_indexed` flag, optimize query path.

3. **Consolidate maintenance** — merge admin + indexer + compactor into
   a single maintenance library/binary with a reconciliation loop.

4. **Fix indexer `size_bytes`** — set actual segment size (one-line).

5. **Size-tiered compaction** — replace count-based selection.

6. **Term-range splitting** — split large segments by term range,
   enable query pruning.
