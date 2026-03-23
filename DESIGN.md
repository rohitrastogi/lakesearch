# LakeSearch: External Full-Text Indices for Parquet

## Overview

LakeSearch builds external full-text search indices on top of Parquet files stored in
cloud object storage. The indices are "bolted on" — existing query engines (DuckDB, Spark,
DataFusion) remain unaware of them and continue to read the Parquet data normally. LakeSearch
provides its own query path that leverages the indices for keyword and prefix search with
BM25 relevance scoring.

All services are stateless. All durable state lives in object storage.

We require Parquet files to have `offset_index` (page locations and
`first_row_index`). The indexer validates this at ingest time and **rejects**
files that lack it with a clear error. This is a hard requirement — the doc
table's `first_row_index` / `row_count` fields and `RowSelection`-based
page-level reads depend on it. `column_index` (per-page min/max stats) is
not required — it could enable additional pruning in the future but nothing
in the current design depends on it.

### Why Not Iceberg (Yet)

Iceberg already has a metadata protocol (snapshots, manifest lists, manifests),
a file inventory, and CAS-based commits. It would be the natural home for
external index metadata. We're not integrating with it for this POC purely to
keep scope small and iteration fast — our metadata protocol is ~200 lines of
JSON serde that we can change freely as the design evolves.

If this POC validates the approach, the natural next step is to propose an
external index extension to the Iceberg spec — adding index artifacts as a
new manifest content type, with the file inventory and commit protocol coming
from Iceberg itself. The segment file format, FST structure, posting list
encoding, and query execution pipeline are all independent of the metadata
layer and would carry over directly.

The harder challenge for full Iceberg support is **delete files**. Iceberg
supports lazy deletes via position delete files (listing `(file, row)` pairs)
and equality delete files (listing column values to exclude). These don't
rewrite data files — the reader reconciles deletes at query time. For a text
index this means:

- Posting lists reference pages that may contain deleted rows. Every query
  would need to check candidate rows against potentially many delete files
  from different snapshots.
- Equality deletes can be on columns the index doesn't know about (e.g.,
  "delete where `user_id = 42`"), requiring evaluation of predicates outside
  the indexed column.
- The index becomes stale when deletes accumulate. Options include checking
  delete files at query time (correct but adds per-query latency), maintaining
  delete bitmaps per segment (complex bookkeeping), or re-indexing affected
  segments when deletes pile up (compaction triggered by deletes).

For append-only workloads — which is the primary use case for log/event data —
none of this applies. That's why the POC assumes append-only and defers delete
support.

## Architecture

```
┌────────────┐     ┌────────────────┐     ┌──────────────┐
│  Indexer   │     │  Compaction    │     │  Query       │
│  Service   │     │  Service       │     │  Service     │
└─────┬──────┘     └───────┬────────┘     └──────┬───────┘
      │                    │                     │
      └────────────────────┼─────────────────────┘
                           │
                    ┌──────▼───────┐
                    │   Object     │
                    │   Storage    │
                    │  (S3/GCS/R2) │
                    └──────────────┘
```

### Indexer

Accepts a batch of newly-appended Parquet files. For each indexed column:

1. Reads every page in the column chunk across all row groups using page indices
2. **Tokenizes each row individually** within every page. This is necessary
   both for determining which pages contain which terms (posting lists) and
   for computing accurate row-level BM25 statistics. For each row:
   - Tokenize the field value
   - For each unique term in this row: increment `doc_frequency[term]`
   - Accumulate `total_tokens += token_count` for this row
   - Record which page (doc_id) this row belongs to
3. Builds in-memory posting lists: term → set of `doc_id`s (page-level). A
   term's posting list includes a page's doc_id if **any** row in that page
   contains the term.
4. Populates the doc table mapping each `doc_id` to its Parquet file, row group,
   page index, `first_row_index`, and `row_count`
5. Writes per-term `doc_frequency` (the number of **rows**, not pages,
   containing the term) and corpus stats (`total_rows`, `total_tokens`) into
   the segment. This is critical for BM25 — using page counts instead of row
   counts would over-estimate IDF and inflate relevance scores for rare terms.
6. Constructs an FST (finite state transducer) as the term dictionary
7. Serializes a **segment file** to object storage
8. Atomically updates the metadata snapshot (CAS on current.json)

When a new column is added to a table's index configuration, the indexer runs a
**backfill** over all existing Parquet files for that column.

#### Parallel Ingestion

Multiple indexer workers can run concurrently as a producer/consumer system.
A queue (SQS, cascadq.) distributes batches of newly-appended Parquet files
to N workers. Each worker independently builds segments, uploads artifacts, and
CAS-commits metadata.

No coordination between workers is needed — the CAS serializes commits. When
two workers finish concurrently, one wins the CAS and the other rebases: it
re-reads the now-updated metadata, adds its manifest list alongside the
winner's, and retries the CAS. No work is wasted because the segments and
manifests are already uploaded; only the small metadata JSON is rewritten.

Under high concurrency, CAS retries should use exponential backoff with jitter
to avoid retry storms. In practice contention is low because indexing (seconds
to minutes) takes much longer than the CAS commit (milliseconds).

### Compaction Service

Runs in the background. Merges small segment files into larger ones:

1. Reads N small segments (for the same indexed column)
2. Merges their FSTs, doc tables, and posting lists (union, remapping doc_ids)
3. Recomputes aggregate BM25 statistics (total rows, total tokens, per-term DF)
4. Writes a single larger segment
5. Atomically updates metadata to replace the N segments with the merged one

This is LSM-tree style: ingestion writes small segments fast, compaction amortizes
the cost of merging them.

### Query Service

REST API. Two modes:

- **Indexed path**: walks the FST to find matching terms, retrieves posting lists,
  intersects/unions as needed, resolves doc_ids to Parquet page locations via the
  doc table, builds a `RowSelection`, and reads only the matching rows. BM25
  scoring is computed at the row level after fetching pages.
- **Brute force path**: full scan of the relevant column pages (fallback when
  indices have not yet been built).

The query service maintains an **in-memory cache** of FSTs, doc tables, metadata,
and Parquet footer/offset_index data. See the Caching section.

### Horizontal Scalability

All three services are fully stateless and scale independently:

```
                    ┌──────────────┐
                    │   Load       │
                    │   Balancer   │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
        ┌─────▼──┐   ┌─────▼──┐   ┌─────▼──┐
        │ Query  │   │ Query  │   │ Query  │
        │   #1   │   │   #2   │   │   #3   │
        └────┬───┘   └────┬───┘   └────┬───┘
             │            │            │
             └────────────┼────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
  ┌─────▼──┐        ┌────▼───┐       ┌─────▼──┐
  │Indexer │        │Indexer │       │Compact │
  │  #1    │        │  #2    │       │  #1    │
  └────┬───┘        └────┬───┘       └────┬───┘
       │                 │                │
       └─────────────────┼────────────────┘
                         │
                  ┌──────▼───────┐
                  │   Object     │
                  │   Storage    │
                  └──────────────┘
```

- **Query services** scale with query load. Each instance is a read-only
  process that fetches from object storage and serves results. No
  coordination between instances — they independently read `current.json`,
  load segments, and execute queries. Per-instance in-memory caches (FSTs,
  doc tables, metadata) warm up independently; a cold instance just makes
  more object storage reads on its first few queries.
- **Indexers** scale with ingest volume as a producer/consumer system
  (see Parallel Ingestion above).
- **Compactor** typically runs as one instance (or a small fixed number)
  since it's background work that doesn't need to be fast. Multiple
  compactors would race on the CAS, which works correctly but wastes effort.

No service holds any durable local state. An instance can be killed and
replaced at any time with no data loss and no coordination. The only cost
of cold-starting a query instance is cache warmup time — optionally
mitigated by preloading FSTs for active segments on startup.

---

## Table Management

Users register tables with LakeSearch and declare which columns to index. The table
configuration is stored in the metadata file and governs what the indexer processes.

### Management API

```
POST   /tables                          # register a new table
GET    /tables                          # list all tables
GET    /tables/{table_id}               # get table config + status
PUT    /tables/{table_id}/columns       # add or drop indexed columns
DELETE /tables/{table_id}               # unregister table
```

### Column Lifecycle

Each indexed column has a status:

- **`active`**: column is fully indexed and queryable.
- **`backfilling`**: column was recently added. The indexer is building indices
  for existing Parquet files. New appends are indexed immediately; queries against
  this column will use indices where available and brute-force scan the rest.
  Progress is derived by walking manifest lists: files not covered by any
  manifest for this column still need work. No separate progress counter is
  stored in metadata (avoids CAS contention with append/compaction commits).
- **`dropped`**: column has been removed from the index configuration. Existing
  segment files for this column are ignored by queries and eligible for GC cleanup.
  No index data needs to be eagerly deleted.

### Registration Example

```json
POST /tables
{
  "table_name": "events",
  "parquet_location": "s3://bucket/data/events/",
  "indexed_columns": [
    { "name": "description", "tokenizer": "whitespace_lowercase" },
    { "name": "error_message", "tokenizer": "whitespace_lowercase" }
  ]
}
```

### Adding a Column

```json
PUT /tables/{table_id}/columns
{
  "add": [
    { "name": "user_agent", "tokenizer": "whitespace_lowercase" }
  ]
}
```

This sets the column status to `backfilling` and triggers the indexer to process
all existing Parquet files for this column. Once backfill completes, the status
transitions to `active`.

### Dropping a Column

```json
PUT /tables/{table_id}/columns
{
  "drop": ["error_message"]
}
```

This sets the column status to `dropped`. The query service immediately stops
using indices for this column. Background GC will eventually clean up the
orphaned segment files.

---

## Metadata Protocol

Inspired by Apache Iceberg but drastically simplified. Append-only, no time travel,
no schema evolution. No directory listings — all file references are explicit
pointers.

### File Hierarchy

```
s3://bucket/lakesearch/tables/{table_id}/
├── metadata/
│   ├── current.json                  # CAS pointer to current metadata
│   ├── metadata-{uuid}.json          # immutable metadata snapshots
│   ├── metadata-{uuid}.json
│   └── ...
├── manifest-lists/
│   ├── manifest-list-{uuid}.json
│   └── ...
├── manifests/
│   ├── manifest-{uuid}.json
│   └── ...
└── segments/
    ├── segment-{uuid}.seg
    └── ...
```

### current.json

The single mutable pointer to the current metadata file. This is the CAS
target — updated atomically via conditional PUT (S3 `If-None-Match` / GCS
generation / Azure ETag). It stores the full path to the current metadata
file, not a version number, because metadata files are UUID-named.

No directory listings are ever needed; readers always go through this pointer.

```json
{
  "metadata_path": "s3://bucket/lakesearch/tables/.../metadata/metadata-a1b2c3d4.json",
  "updated_at": "2026-03-22T17:24:18Z"
}
```

Metadata files are UUID-named (not sequentially numbered) to prevent a race
where two concurrent writers both target the same file name. With UUID naming,
each writer creates a globally unique file — the CAS on `current.json`
determines which one becomes the active metadata. The loser's metadata file
becomes an orphan cleaned up by GC.

### Metadata File (metadata-{uuid}.json)

The metadata file contains only table configuration and a list of manifest
list pointers. It does **not** contain a data file inventory — that is derived
from manifest lists (see below).

```json
{
  "format_version": 1,
  "table_id": "550e8400-e29b-41d4-a716-446655440000",
  "table_name": "events",
  "location": "s3://bucket/lakesearch/tables/550e8400.../",
  "indexed_columns": [
    {
      "name": "description",
      "tokenizer": "whitespace_lowercase",
      "status": "active"
    },
    {
      "name": "error_message",
      "tokenizer": "whitespace_lowercase",
      "status": "active"
    },
    {
      "name": "user_agent",
      "tokenizer": "whitespace_lowercase",
      "status": "backfilling"
    },
    {
      "name": "old_field",
      "tokenizer": "whitespace_lowercase",
      "status": "dropped"
    }
  ],
  "snapshot": {
    "timestamp_ms": 1711100000000,
    "manifest_lists": [
      "s3://bucket/lakesearch/tables/.../manifest-lists/manifest-list-aaa.json",
      "s3://bucket/lakesearch/tables/.../manifest-lists/manifest-list-bbb.json",
      "s3://bucket/lakesearch/tables/.../manifest-lists/manifest-list-ccc.json"
    ]
  }
}
```

The metadata file is flat and stable — it only grows when manifest lists
aren't being compacted, which is already the compactor's job to prevent.
Each append or backfill commit writes a new metadata file that adds one
manifest list pointer. The metadata file never contains per-file data, so
its size stays proportional to the number of manifest lists, not the number
of Parquet files.

### Manifest List (manifest-list-{uuid}.json)

A manifest list groups manifests that were written together in one indexing
operation. It also carries a `data_files` array listing the Parquet files
processed in that batch. This makes manifest lists the authoritative source
for the full file inventory and per-file index coverage.

```json
{
  "job_kind": "append",
  "batch_id": "sha256:a1b2c3...",
  "data_files": [
    {
      "path": "s3://bucket/data/events/part-00042.parquet",
      "file_size_bytes": 134217728,
      "row_count": 1000000
    }
  ],
  "manifests": [
    {
      "manifest_path": "s3://bucket/.../manifests/manifest-001.json",
      "indexed_column": "description",
      "segment_count": 1,
      "term_stats": {
        "min_term": "aardvark",
        "max_term": "zebra",
        "term_count": 12450
      }
    },
    {
      "manifest_path": "s3://bucket/.../manifests/manifest-002.json",
      "indexed_column": "error_message",
      "segment_count": 1,
      "term_stats": {
        "min_term": "authentication",
        "max_term": "timeout",
        "term_count": 3200
      }
    }
  ]
}
```

For compaction manifest lists, a `replaces` field identifies the manifest lists
being superseded. The compacted manifest list must carry forward:
- The **union** of `data_files` from all replaced manifest lists (otherwise
  file history is lost and backfill sees false gaps).
- **Untouched manifests for other columns.** Because a manifest list can
  contain manifests for multiple columns, compacting one column's segments
  must not retire the other columns' manifests. The compacted manifest list
  includes the merged manifest for the compacted column plus the original
  manifests for all other columns from the replaced manifest lists.

```json
{
  "job_kind": "compact",
  "compacted_column": "description",
  "replaces": [
    "s3://bucket/.../manifest-lists/manifest-list-aaa.json",
    "s3://bucket/.../manifest-lists/manifest-list-bbb.json"
  ],
  "data_files": [
    {
      "path": "s3://bucket/data/events/part-00001.parquet",
      "file_size_bytes": 134217728,
      "row_count": 1000000
    },
    {
      "path": "s3://bucket/data/events/part-00002.parquet",
      "file_size_bytes": 128974848,
      "row_count": 950000
    }
  ],
  "manifests": [
    {
      "manifest_path": "s3://bucket/.../manifests/manifest-compacted-001.json",
      "indexed_column": "description",
      "segment_count": 1,
      "term_stats": { "min_term": "aardvark", "max_term": "zebra", "term_count": 24000 }
    },
    {
      "manifest_path": "s3://bucket/.../manifests/manifest-002.json",
      "indexed_column": "error_message",
      "segment_count": 1,
      "term_stats": { "min_term": "authentication", "max_term": "timeout", "term_count": 3200 }
    }
  ]
}
```

The `batch_id` is a deterministic hash of **stable source identity only**:
the sorted appended file paths plus an upstream identifier (e.g., a lake
snapshot ID or an explicit append ID from the queue message). It must not
include the current LakeSearch metadata snapshot, because that changes on
every unrelated commit — a retry after a concurrent append would produce a
different `batch_id` and defeat dedup.

When committing metadata, the writer checks that no existing manifest list
in the snapshot already has the same `batch_id`. If a queue redelivers the
same append batch, the retry detects the duplicate and skips the commit.
Without this, at-least-once delivery would produce duplicate segments and
double-count rows in BM25 scoring.

### Deriving File Inventory and Index Coverage

Everything the query service and backfill worker need is derived by walking
manifest lists and their manifests — no separate data file inventory is needed:

- **Full file set**: union of `data_files` across all manifest lists.
- **Per-file index coverage**: a file is "indexed for column X" if it appears
  in the `parquet_files` array of a **manifest** (not manifest list) whose
  `indexed_column` is X. This is important because compacted manifest lists
  carry forward untouched manifests for other columns — manifest-list-level
  membership does not imply coverage for all columns in that list.
- **Backfill progress**: files in `data_files` that do not appear in any
  manifest for a `backfilling` column still need work. The backfill worker
  processes a batch of uncovered files, writes a new manifest list (with
  `data_files` listing only that batch), and commits — O(batch) per commit,
  not O(corpus).
- **Brute-force fallback**: the query service already loads manifest lists
  and manifests to find segments. It gets the file set and per-column
  coverage for free from the same read.

This eliminates write amplification: a backfill commit writes a manifest list
covering only its batch, not the entire file inventory. An append commit only
adds the new files. The metadata file itself is just table config + manifest
list pointers and never changes size based on file count.

### Manifest File (manifest-{uuid}.json)

Maps segment files to the Parquet files they index.

```json
{
  "indexed_column": "description",
  "segments": [
    {
      "segment_path": "s3://bucket/.../segments/segment-xyz.seg",
      "size_bytes": 2097152,
      "term_count": 12450,
      "doc_count": 5000,
      "total_rows": 500000,
      "total_tokens": 2500000,
      "parquet_files": [
        {
          "file_ordinal": 0,
          "path": "s3://bucket/data/events/part-00001.parquet",
          "file_size_bytes": 134217728,
          "row_group_count": 10
        },
        {
          "file_ordinal": 1,
          "path": "s3://bucket/data/events/part-00002.parquet",
          "file_size_bytes": 128974848,
          "row_group_count": 9
        }
      ]
    }
  ]
}
```

`file_ordinal` is a segment-local identifier used in the doc table to reference
Parquet files compactly. It is assigned sequentially starting from 0 per segment.

`total_rows` and `total_tokens` are used for BM25 scoring (average document length
computation) and can be aggregated across segments at query time.

---

## Segment File Format

The segment file is the core index artifact. It is a binary file designed for:

- Sequential writes (indexer writes it once, never modifies)
- Random-access reads (query service seeks to specific posting lists)
- Block-level compression friendly to object storage range requests

Each segment indexes a single column.

### Design Decisions

#### Dense doc_id space (not inline PageRef tuples)

Each page indexed by a segment is assigned a dense, segment-local `doc_id`
(0, 1, 2, ..., N-1). Posting lists contain sorted `doc_id` arrays. A separate
**doc table** maps each `doc_id` back to its Parquet location.

We chose this over embedding `(file_id, row_group, page)` tuples directly in
posting lists for several reasons:

- **Compact posting lists.** Each entry is a single `u32` (4 bytes) instead of
  an 8-byte tuple. Posting lists are the largest section of a segment and are
  read on every query, so this matters.
- **Better delta encoding.** Dense doc_ids produce small, regular deltas (often
  just 1), which compress extremely well with varint or bit-packing.
- **Faster intersection.** Intersecting sorted `u32` arrays is a well-studied
  problem with efficient implementations (galloping search, SIMD). Comparing
  multi-field tuples has more overhead.
- **Natural place for page metadata.** The doc table stores `first_row_index`
  and `row_count` per page, which are needed for cross-column intersection.
  Without a doc table, this metadata would need a separate structure anyway.

The tradeoff is one level of indirection: after boolean evaluation produces a
set of `doc_id`s, we look them up in the doc table to get Parquet locations.
The doc table is small (20 bytes per page) and loaded into memory alongside
the FST, so this lookup is fast.

#### No page-map sidecar files

An alternative design (explored during development) materializes Parquet page
metadata (byte offsets, `first_row_index`, dictionary page offsets) into separate
sidecar files during indexing. We decided against this because:

- **The arrow-rs `RowSelection` API handles dictionary pages transparently.**
  Our spike confirmed that `ParquetRecordBatchReaderBuilder` with `RowSelection`
  correctly decodes pages regardless of encoding (dictionary, plain, or mixed).
  The reader automatically fetches dictionary pages when needed. We never have to
  manage dictionary page offsets ourselves.
- **Column projection works seamlessly with `RowSelection`.** Combining
  `ProjectionMask` (read only requested columns) with `RowSelection` (read only
  matching row ranges) gives us precise, efficient I/O with no custom page
  decoding.
- **Fewer files to manage.** A sidecar per Parquet file means thousands of
  additional objects in storage, each requiring creation, tracking in manifests,
  and GC. Object storage operations (not bytes) are often the bottleneck.
- **Caching closes the cold-read gap.** The Parquet footer + `offset_index` is
  small and immutable per file. Caching parsed metadata in the query service
  avoids repeated reads after the first access.

The `offset_index` (read from Parquet at query time, cached) provides
`first_row_index` per page, which is sufficient for cross-column intersection.
The doc table stores this alongside each `doc_id`, so cross-column intersection
can often proceed without touching Parquet metadata at all.

### Layout

Sections are ordered so that write-once bulk data (posting blocks, FSTs) is
at the head and frequently-read metadata (doc table, term info, corpus stats,
footer) is at the tail. This allows a single speculative tail read to load
everything needed for query planning without knowing section sizes in advance.

```
┌──────────────────────────────────────────────────────────┐
│ Magic: "LKSR" (4 bytes)                                  │
│ Version: u16 (2 bytes)                                   │
│ Flags: u16 (2 bytes)                                     │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  File Table Section  (variable size, rarely read)        │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Num files: u32                                     │  │
│  │ Per file_ordinal:                                  │  │
│  │   path_offset: u32  (into string pool)             │  │
│  │   path_length: u16                                 │  │
│  │   row_group_count: u16                             │  │
│  │ String pool: [u8]                                  │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Posting Blocks  (bulk data, read per-term on demand)    │
│  ┌────────────────────────────────────────────────────┐  │
│  │   Block 0: [compressed doc_id list]                │  │
│  │   Block 1: [compressed doc_id list]                │  │
│  │   ...                                              │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Forward FST Section  (loaded on first access, cached)   │
│  ┌────────────────────────────────────────────────────┐  │
│  │ FST byte length: u64                               │  │
│  │ FST data (built by `fst` crate)                    │  │
│  │   Maps: term (bytes) → term_ordinal (u64)          │  │
│  │   Used for: exact term lookup, prefix queries      │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Reverse FST Section  (loaded on first access, cached)   │
│  ┌────────────────────────────────────────────────────┐  │
│  │ FST byte length: u64                               │  │
│  │ FST data (built by `fst` crate)                    │  │
│  │   Maps: reversed term (bytes) → term_ordinal (u64) │  │
│  │   Used for: suffix queries                         │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
├──────────────────────────────────────────────── TAIL ────┤
│  (everything below here fits in one speculative read)    │
│                                                          │
│  Term Info Table  (fixed-width, loaded into memory)      │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Num terms: u32                                     │  │
│  │ Per term_ordinal:                                  │  │
│  │   posting_offset: u64   (into posting blocks)      │  │
│  │   posting_length: u32   (byte length)              │  │
│  │   doc_frequency:  u32   (# rows containing term)   │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Doc Table Section  (fixed-width, loaded into memory)    │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Num docs (pages): u32                              │  │
│  │                                                    │  │
│  │ Doc Table (num_docs entries, 20 bytes each):       │  │
│  │   file_ordinal:    u32                             │  │
│  │   row_group:       u16                             │  │
│  │   page_index:      u16                             │  │
│  │   first_row_index: u64                             │  │
│  │   row_count:       u32                             │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Corpus Stats Section  (16 bytes)                        │
│  ┌────────────────────────────────────────────────────┐  │
│  │ total_rows: u64     (rows across indexed files)    │  │
│  │ total_tokens: u64   (total tokens for avg_dl)      │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Footer (fixed size: 56 bytes)                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │ File table section offset: u64                     │  │
│  │ Doc table section offset: u64                      │  │
│  │ Forward FST section offset: u64                    │  │
│  │ Reverse FST section offset: u64                    │  │
│  │ Posting lists section offset: u64                  │  │
│  │ Corpus stats section offset: u64                   │  │
│  │ Segment checksum (CRC32): u32                      │  │
│  │ Magic: "LKSR" (4 bytes)                            │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Reading a Segment

1. Read the last 56 bytes (footer) to get section offsets
2. Load the doc table into memory (small — 20 bytes × num_pages)
3. Load the forward FST (and optionally the reverse FST) into memory
4. For a query term, look up the forward FST to get `term_ordinal`.
   For a suffix query, reverse the suffix and prefix-search the reverse FST.
5. Read `term_info_table[term_ordinal]` to get posting offset and `doc_frequency`
6. Seek to posting data, decompress and decode the `doc_id` list
7. Resolve `doc_id`s through the doc table to get Parquet page locations
8. Read corpus stats for BM25 parameters (`total_rows`, `total_tokens`)

### I/O Optimization

Object storage round-trips are expensive (typically 10-50ms per request).
The design minimizes them at two levels:

**Parquet reads: handled by arrow-rs.** The `parquet` crate's async reader
uses `object_store::get_ranges()` internally to coalesce nearby byte ranges
when reading column chunks. When we provide a `RowSelection`, the reader
skips pages outside the selection and batches the remaining reads. We get
this for free.

**Segment reads: our responsibility.** Arrow knows nothing about our segment
format. We optimize segment I/O in two ways:

1. **Bulk footer load.** On first access to a segment, read the tail of the
   file in one request sized to cover the footer + corpus stats + doc table +
   term info table (these are all at the end, before the footer). The exact
   size is unknown before reading the footer, so we use a speculative read
   (e.g., last 256KB) and parse what we get. If the doc table or FST didn't
   fit, do a follow-up read. For most segments, one read loads everything
   except the posting blocks and FST — and the FST can be included if the
   speculative size is large enough.

2. **Concurrent segment loading.** A query touching N segments issues all N
   segment loads concurrently (`FuturesUnordered` / `join_all`). Each is an
   independent file, so they can't be coalesced but they can overlap.

3. **Batched posting reads.** For multi-term queries, collect all posting
   list `(offset, length)` ranges from the term info table, sort them, and
   issue a single `object_store::get_ranges()` call to fetch them all from
   the segment file. Adjacent or nearby ranges are coalesced by the
   `object_store` implementation.

### Posting List Compression

Each posting list is encoded as a sequence of **blocks** of up to 128 `doc_id`s:

```
┌──────────────────────────────────────┐
│ Block Header (15 bytes)              │
│   num_docs: u16                      │
│   min_doc_id: u32     ← skip-ahead   │
│   bit_width: u8       ← bits per delta│
│   flags: u8           ← bit 0: LZ4   │
│   compressed_size: u32               │
│   uncompressed_size: u16             │
├──────────────────────────────────────┤
│ Compressed Data                      │
│   delta-encoded doc_ids, bit-packed  │
│   (optionally LZ4-compressed)        │
└──────────────────────────────────────┘
```

Encoding within a block:
1. Delta-encode the sorted `doc_id` array (dense IDs → small deltas, often 1)
2. Determine `bit_width`: minimum bits needed to represent the largest delta
3. Bit-pack the deltas into `ceil(num_docs * bit_width / 8)` bytes
4. If `flags & 0x01`: apply LZ4 block compression on the bit-packed data.
   `compressed_size` is the LZ4 output size; `uncompressed_size` is needed
   for LZ4 decompression. If LZ4 is not applied, `compressed_size` equals
   the bit-packed size and `uncompressed_size` is ignored.

The `bit_width` field is always stored in the header so the decoder knows
how to unpack deltas regardless of whether LZ4 is applied.

The block structure enables skip-ahead during intersection: read `min_doc_id`
from each block header to decide whether to decompress it.

Because `doc_id`s are dense and sorted, delta encoding is highly effective:
most deltas are 1 (consecutive pages) or small integers (sparse hits). This
compresses significantly better than delta-encoding three separate fields from
an 8-byte `(file_id, row_group, page)` tuple.

---

## BM25 Scoring

LakeSearch uses BM25 for relevance ranking. Scoring is computed at the **row level**
at query time, not at the page level.

### Why Row-Level, Not Page-Level

Pages are an I/O access unit — an arbitrary chunk of column values determined by the
Parquet writer's page size settings. They are not semantically meaningful documents.
The actual documents are rows. The index uses pages only for pruning which data to
fetch; scoring must happen after decoding individual rows.

### Scoring Flow

1. **From the segment file**: read `doc_frequency(t)` (per term, from term info
   table) and corpus stats (`total_rows`, `total_tokens` → `avg_dl`)
2. **Fetch matching pages** from Parquet files using `RowSelection`
3. **For each row** in a fetched page: tokenize the field value, compute per-row
   term frequency (`tf`), and score using BM25

### BM25 Formula

```
score(t, row) = IDF(t) * (tf(t, row) * (k1 + 1)) / (tf(t, row) + k1 * (1 - b + b * dl / avg_dl))
```

Where:
- `tf(t, row)` = term frequency of term `t` in this row (computed at query time
  by tokenizing the row's field value)
- `dl` = document length = total token count of this row's field value
- `avg_dl` = `total_tokens / total_rows` (from corpus stats in segment)
- `IDF(t) = ln(1 + (N - df(t) + 0.5) / (df(t) + 0.5))`
- `N` = `total_rows` from corpus stats
- `df(t)` = `doc_frequency` from term info table
- `k1 = 1.2`, `b = 0.75` (standard defaults)

### Cross-Segment Scoring

When a query spans multiple segments, we need global statistics for accurate IDF.
Two approaches:

1. **Segment-local scoring** (MVP): compute BM25 within each segment independently,
   merge results. Acceptable when segments are large enough that local stats
   approximate global stats.
2. **Global stats aggregation**: before scoring, aggregate `N` and `df(t)` across
   all segments. More accurate but requires an extra pass. Can be done efficiently
   since manifest files store `total_rows` and the term info table stores per-term
   `doc_frequency`.

We start with approach 1 for the MVP and add approach 2 when segment count is high.

---

## Query Model

### Query Language

Queries can search across **multiple indexed columns** with boolean operators.
Searches on different columns can be combined with AND/OR/NOT at the top level.

The query language provides both low-level primitives (`term`, `prefix`, `and`,
`or`, `not`) and a higher-level `match` shorthand that mirrors Elasticsearch's
most common query pattern.

#### Match (high-level shorthand)

The `match` node takes a raw text string, tokenizes it using the column's
configured tokenizer, and combines the resulting terms with an implicit boolean
operator (default: `or`, like Elasticsearch).

```json
{
  "table": "events",
  "select": ["timestamp", "user_id", "description"],
  "search": {
    "column": "description",
    "match": "connection timeout",
    "operator": "and"
  },
  "limit": 100,
  "score": true
}
```

This is equivalent to:

```json
{
  "search": {
    "column": "description",
    "query": {
      "and": [
        { "term": "connection" },
        { "term": "timeout" }
      ]
    }
  }
}
```

When `operator` is `"or"` (or omitted), any token matching is sufficient.
When `operator` is `"and"`, all tokens must match. This covers the vast
majority of real-world text search queries with minimal ceremony.

#### Full boolean query (low-level)

For complex queries, the full boolean tree is available:

```json
{
  "table": "events",
  "select": ["timestamp", "user_id", "description", "error_message"],
  "search": {
    "and": [
      {
        "column": "description",
        "query": {
          "and": [
            { "term": "error" },
            { "not": { "term": "heartbeat" } },
            { "or": [
              { "term": "timeout" },
              { "prefix": "connect" }
            ]}
          ]
        }
      },
      {
        "column": "error_message",
        "query": { "match": "ECONNREFUSED", "operator": "and" }
      }
    ]
  },
  "limit": 100,
  "score": true
}
```

#### Multi-column match shorthand

For the common case of searching the same text across multiple columns:

```json
{
  "table": "events",
  "select": ["timestamp", "description", "error_message"],
  "search": {
    "multi_match": "connection refused",
    "columns": ["description", "error_message"],
    "operator": "and"
  },
  "limit": 50
}
```

This expands to an OR across columns (any column matching is sufficient),
with AND within each column (all tokens must appear). Per-column BM25 scores
are summed for rows that match in multiple columns.

### Query Response

Responses include query statistics for debugging and understanding index
effectiveness:

```json
{
  "rows": [ ... ],
  "stats": {
    "segments_touched": 3,
    "candidate_pages": 12,
    "rows_scanned": 360,
    "rows_matched": 4,
    "elapsed_ms": 42
  }
}
```

### Single-Column Query Nodes

Within a single column, the query supports:

- **Term**: exact keyword match (after tokenization)
- **Prefix**: match all terms sharing a prefix (forward FST prefix iteration → union)
- **Suffix**: match all terms ending with a suffix (reverse the suffix, prefix-search the reverse FST → union)
- **Match**: tokenize a raw string, combine tokens with AND or OR
- **And**: intersection of result sets
- **Or**: union of result sets
- **Not**: negation — enforced at row-level verification, not page-level

Term, prefix, suffix, and match nodes operate on `doc_id` sets from segments
for the same column. AND and OR are standard sorted-array intersection/union
on `u32` values.

**Important:** all page-level boolean operations are **approximate**. They
identify candidate pages that *may* contain matching rows, but a page-level
AND does not guarantee every row in the page satisfies the full predicate. The
row-level verification step (see execution pipeline) evaluates the complete
boolean query against each fetched row to eliminate false positives.

#### NOT semantics

`not` must appear as a child of `and`:

```json
{ "and": [{ "term": "error" }, { "not": { "term": "heartbeat" } }] }
```

A bare `not` at the top level is rejected — negation without a positive
constraint would require scanning the entire corpus.

**NOT is not applied during page-level boolean evaluation.** Subtracting
pages containing "heartbeat" from pages containing "error" is unsound: a page
can contain rows with "error" (but not "heartbeat") alongside rows with both
terms. Page-level set difference would incorrectly drop the entire page,
losing valid matches.

Instead, during page-level evaluation, NOT clauses are ignored — only the
positive clauses (AND/OR of terms) determine candidate pages. The NOT is then
enforced during row-level verification: after fetching and tokenizing each
candidate row, the full boolean AST (including NOT) is evaluated to confirm
the row truly matches.

`not` clauses do not contribute to BM25 scoring (like Elasticsearch's
`must_not`). They only filter.

### Multi-Column Intersection

When a query combines searches on different columns with AND, a cross-column
intersection is needed. Different columns have different page boundaries — our
spike confirmed this: with byte-size page limits, `description` (long strings) had
25 pages per row group while `category` (short strings) had 9 and `id` (Int32) had
just 4, all with completely different row ranges.

Cross-column intersection uses the **doc table** and Parquet `offset_index` to
resolve page-level row ranges:

1. For each column's search, evaluate boolean query → set of `doc_id`s
2. Resolve each `doc_id` through the doc table to get `first_row_index` and
   `row_count`, producing row ranges per column
3. Intersect row ranges across columns — find overlapping intervals
4. Build a `RowSelection` from the overlapping intervals
5. Read matching rows using `ParquetRecordBatchReaderBuilder` with the
   `RowSelection` and a `ProjectionMask` for requested columns

For columns where `first_row_index` is already in the doc table (populated
at index time from the Parquet `offset_index`), cross-column intersection
proceeds **without any additional Parquet metadata reads**.

**Example**: If `description` doc 3 covers rows [60, 80) and `category` doc 1
covers rows [60, 120), and both match their respective search terms, the
intersection yields rows [60, 80). We build a `RowSelection` that selects
exactly those rows.

For multi-column OR, union the row ranges and return rows matching either predicate.

BM25 scores in multi-column queries are computed per-column and summed across
matching columns for each row.

### Query Execution Pipeline

1. **Plan**: Parse query, load metadata, identify relevant segments per column.
   Expand `match` and `multi_match` nodes into boolean trees of `term` nodes.
2. **Selectivity Estimation**: For each term node, look up `doc_frequency`
   from the term info table (one read per term, no posting list decoding
   needed). Compute estimated selectivity:
   - Single term: `df / total_rows`
   - AND: `min(df_a, df_b, ...) / total_rows` (intersection can't exceed
     the smallest posting list)
   - OR: `sum(df_a, df_b, ...) / total_rows` (capped at 1.0)
   - If estimated selectivity exceeds a threshold (e.g., >30% of rows),
     **skip the index and fall back to brute force scan** for this segment.
     The overhead of decoding posting lists, building RowSelection, and
     verifying rows page-by-page exceeds the cost of a sequential scan
     when most pages match.
3. **Term Resolution**: For each term/prefix/suffix node per column:
   - Term: forward FST lookup → single posting list
   - Prefix: forward FST prefix iterator → multiple posting lists → union
   - Suffix: reverse FST prefix iterator (on reversed suffix) → term_ordinals → posting lists → union
4. **Page-Level Candidate Selection** (approximate):
   - AND: intersect `doc_id` lists (sorted merge intersection)
   - OR: union `doc_id` lists (sorted merge union)
   - NOT: **skipped** at this stage (see NOT semantics above)
   - Result: candidate `doc_id` set per column (may contain false positives)
5. **Doc Table Resolution**: look up doc table to get `first_row_index` and
   `row_count` for each candidate doc_id
6. **Cross-Column Intersection** (if multi-column query):
   - Intersect/union row ranges across columns
7. **RowSelection Construction**: build `RowSelection` from candidate row
   ranges, grouped by Parquet file and row group
8. **Page Fetch**: read candidate rows using `ParquetRecordBatchReaderBuilder`
   with `RowSelection` + `ProjectionMask` (include all searched columns)
9. **Row-Level Verification**: for each fetched row, tokenize the searched
   column values and evaluate the **full boolean AST** (including NOT clauses)
   against the actual row data. Discard rows that don't match. This eliminates
   false positives from page-level approximation.
10. **Scoring / Output** (depends on query mode):
    - **Filter mode** (`score: false` or omitted): emit verified rows
      immediately. No TF computation, no IDF lookup, no BM25 math. If `limit`
      is specified, stop after N verified rows (no ranking — first-N, not
      top-N). Fully streamable in all cases.
    - **Search mode** (`score: true`): for verified rows, compute per-row TF
      and score with BM25 using precomputed DF and corpus stats. If `limit`
      is specified, maintain a top-K heap and emit the top-K by score after
      all candidates are evaluated (requires buffering). Without `limit`,
      stream scored rows in arbitrary order.
11. **Projection**: return only the requested `select` columns (with scores
    if search mode)

### Brute Force Fallback

Used when indices are unavailable (column is `backfilling` and some files aren't
indexed yet). The query service derives per-file index coverage by walking
manifest lists (see "Deriving File Inventory and Index Coverage" above) — no
directory listings required.

1. Walk manifests for the searched column and collect their `parquet_files`
   to determine which files are indexed. Files not in any such manifest are
   un-indexed.
2. For indexed files: use the indexed path (page-level candidates → row
   verification)
3. For un-indexed files: read column pages sequentially, apply the full boolean
   predicate row-by-row
4. Merge and deduplicate results from both paths

---

## Concurrency Control

### Atomic Metadata Updates

Both the indexer and compaction service need to update metadata atomically.
The protocol uses optimistic concurrency with no directory listings:

1. Read `current.json`, capture its ETag/generation
2. Read the metadata file it points to
3. Compute new metadata (add manifest list for append, rewrite for compaction)
4. Write `metadata-{uuid}.json` — UUID-named, so globally unique. Two
   concurrent writers will never target the same file.
5. Conditionally update `current.json` to point to the new metadata file,
   conditioned on the ETag/generation still matching (S3 conditional PUT /
   GCS generation match)
6. If the conditional write fails: re-read, rebase, retry from step 1.
   The orphaned `metadata-{uuid}.json` from the failed attempt is cleaned
   up by GC.

### Why This Works for Append + Compaction

- **Append**: adds a new manifest list to the snapshot's list. Never modifies
  existing segments or manifests.
- **Compaction**: adds a new manifest list (with `job_kind: "compact"` and a
  `replaces` field) and removes the replaced manifest lists from the snapshot.
  The underlying segment files being replaced are never deleted until the new
  metadata is committed.
- **Conflict**: If an append and compaction race, one will fail the CAS. The
  loser retries:
  - If append loses: just re-read metadata, add its manifest list to the
    (now compacted) snapshot. Trivial rebase.
  - If compaction loses: re-read metadata. The new manifest list from the
    append doesn't conflict with the segments being compacted (they're
    disjoint). Compaction can include it in the new snapshot or just rebase
    around it.

### Garbage Collection

The compaction service is responsible for periodic GC as part of its background
work. Orphaned files come from three sources:

1. **Compaction**: old segments and manifests replaced by merged versions
2. **Dropped columns**: segment files for columns with `status: "dropped"`
3. **Failed indexer/compactor runs**: artifacts uploaded before a crash or CAS
   failure that were never committed to metadata

All three cases produce the same thing: files in object storage not referenced
by the current metadata. Because the CAS on `current.json` is the single
atomic commit point, any unreferenced file is safe to delete (after a delay).
Failed runs don't require special tracking — the orphaned artifacts are simply
invisible to queries and get swept up by the same GC pass that handles
compaction and dropped columns.

GC runs periodically as part of the compaction service's loop:

1. Read `current.json` and the last N metadata files (snapshot retention)
2. For each retained metadata, walk manifest lists → manifests → segments
   to build the **retained file set** (union across all retained snapshots)
3. List files in `segments/`, `manifests/`, `manifest-lists/`, `metadata/`
4. Delete any file not in the retained set

#### Snapshot Retention

Queries pin a metadata snapshot at start. As long as that snapshot is
retained by GC, all files it references are guaranteed to exist. GC
never deletes files referenced by a retained snapshot.

Two configurable retention policies (both enforced, like Iceberg):

- `min_snapshots_to_keep`: retain at least this many metadata files
  (default: 10). Protects against burst compaction that rapidly produces
  new snapshots.
- `max_snapshot_age`: retain any metadata file younger than this duration
  (default: 1 hour). Protects long-running queries.

A metadata file is retained if it satisfies **either** policy. Files
referenced only by expired snapshots are eligible for deletion.

This eliminates the need for grace period coordination between the
compactor and query service. The query service enforces a hard
**query timeout** (configurable, default 5 minutes) to bound execution,
but this is independent of GC — snapshot retention is what guarantees
file availability.

---

## Caching

The query service maintains an in-memory cache to avoid repeated object storage
reads. Cached items and their staleness strategy:

### What to Cache

| Item | Size | TTL / Invalidation Strategy |
|------|------|-----------------------------|
| Current metadata version (from current.json) | Tiny | Poll every N seconds (e.g. 5s). Stale reads just use slightly old index state — safe because indices are append-only. |
| Metadata file (metadata-{uuid}.json) | Small | Immutable once written. Cache keyed by path (from current.json). Evict when current.json points to a new metadata file. |
| Manifest lists and manifests | Small | Immutable once written. Cache indefinitely, evict via LRU when memory pressure. |
| FSTs (from segment files) | Medium (typically KB–low MB per segment) | Immutable. Cache keyed by segment path. Evict via LRU. Highest-value cache items. |
| Doc tables (from segment files) | Small-medium (20 bytes × pages per segment) | Immutable. Cache keyed by segment path. Needed for every query that hits this segment. |
| Parquet footer + offset_index | Small | Immutable per file. Cache keyed by file path. Needed only for cross-column queries if doc table doesn't already have `first_row_index` (it does). |
| Posting list blocks | Large | Immutable. Optional — only cache hot blocks under LRU. |

### Staleness Safety

All cached items except `current.json` are **immutable files**. Once written,
they never change. This means cache invalidation is trivial:

- A new metadata version means new manifest lists and potentially new segments.
  Old cached manifests/FSTs remain valid as long as the current metadata still
  references them.
- When a new metadata version is detected, load the new manifest lists and
  segments. Old entries remain in cache until evicted by LRU or until the
  metadata version that referenced them is no longer current (at which point
  GC may delete the underlying files).

The only race condition: a cached segment file could be GC'd while still in cache.
Mitigate by ensuring GC delay is much longer than max query execution time.

### Cache Implementation

Use an LRU cache with a configurable memory budget (e.g. 256MB default). Priority:
1. FSTs (highest hit rate)
2. Doc tables (needed for every query, enable cross-column intersection)
3. Manifest files
4. Posting list blocks (largest, lowest priority)

---

## Tokenization

MVP tokenizer: `whitespace_lowercase`

1. Split on Unicode whitespace and punctuation (`char::is_alphanumeric` boundaries)
2. Lowercase (Unicode-aware)
3. Normalize to NFC
4. Filter tokens shorter than 1 character or longer than 256 bytes
5. Each surviving token becomes a term in the posting list
6. Count occurrences per row for BM25 `doc_frequency` aggregation at index time

Future tokenizers (not MVP): stemming, n-grams, language-specific.

---

## Parquet Page Access

The system requires page-level random access within Parquet files. `offset_index`
is required (validated at ingest — see Overview). `column_index` is optional and
not used by the current design.

### RowSelection-Based Access

Rather than performing raw byte-range page reads, LakeSearch uses the arrow-rs
`RowSelection` API to read specific row ranges from Parquet files. This was
validated by our spike:

- **Dictionary encoding**: `RowSelection` correctly handles dictionary-encoded
  columns. The reader automatically fetches dictionary pages as needed — no
  manual dictionary page management required.
- **Mixed encodings**: files with some columns dictionary-encoded and others
  plain-encoded work correctly with a single `RowSelection`.
- **Non-contiguous reads**: scattered page selections (e.g., pages [0, 8, 16]
  out of 17) work correctly, and the reader skips intervening pages.
- **Column projection**: combining `ProjectionMask` with `RowSelection` reads
  only the requested columns for only the matching rows.

This means the query path is:
1. Resolve `doc_id`s to `(file_ordinal, row_group, first_row_index, row_count)`
   via the doc table
2. Build a `RowSelection` from the row ranges
3. Open `ParquetRecordBatchReaderBuilder` with `RowSelection` + `ProjectionMask`
4. Iterate record batches — the reader handles all encoding details

### API Surface (arrow-rs `parquet` crate v54)

- `ParquetMetaDataReader::new().with_page_indexes(true)` — load page indices
- `ArrowReaderOptions::new().with_page_index(true)` — enable page-index-aware reads
- `RowSelection::from(vec![RowSelector::select(n), RowSelector::skip(m), ...])`
- `ParquetRecordBatchReaderBuilder::with_row_selection(selection)`
- `ParquetRecordBatchReaderBuilder::with_projection(mask)`
- `OffsetIndexMetaData::page_locations()` → `Vec<PageLocation>` where each has
  `offset`, `compressed_page_size`, `first_row_index`

### Cross-Column Page Boundaries

Different columns within the same row group have **different page boundaries**.
This was confirmed by our spike:
- With byte-size page limits (512 bytes), `description` (long strings) produced
  25 pages of 20 rows each, `category` (short strings) produced 9 pages of 60
  rows, and `id` (Int32) produced just 4 pages of 130 rows.
- With row-count page limits, all columns align to the same boundaries.

Real-world Parquet files typically use byte-size page limits, so page boundaries
will diverge across columns. Cross-column intersection uses `first_row_index` and
`row_count` from the doc table to compute row-range overlaps.

---

## Arrow Data Interface

The query service exposes search results as Arrow data, enabling direct
integration with OLAP engines (DuckDB, DataFusion, Polars) without JSON
serialization overhead or data re-scanning. Two interfaces are provided:

### Arrow IPC over HTTP

The REST search endpoint supports `Accept: application/vnd.apache.arrow.stream`
to return results as an Arrow IPC stream instead of JSON. This is the simplest
integration path.

```python
import pyarrow.ipc as ipc
import duckdb
import requests

response = requests.post(
    "http://localhost:8080/v1/tables/events/search",
    json={
        "search": {"column": "description", "match": "connection timeout", "operator": "and"},
        "select": ["timestamp", "service", "description", "response_time_ms"]
    },
    headers={"Accept": "application/vnd.apache.arrow.stream"}
)

table = ipc.open_stream(response.content).read_all()

# Arrow-native into DuckDB — data is pre-filtered, no re-verification needed
conn = duckdb.connect()
conn.sql("""
    SELECT service, count(*) as cnt, avg(response_time_ms) as avg_rt
    FROM table
    WHERE timestamp >= '2026-03-01'
    GROUP BY service
    ORDER BY cnt DESC
""").show()
```

### Arrow Flight (gRPC)

For streaming large result sets and native SQL integration with DuckDB, the
query service also exposes an Arrow Flight endpoint. The Flight protocol maps
to our query pipeline:

- **`GetFlightInfo(command)`**: client sends a search query as the command
  payload. Server parses the query, plans execution, and returns `FlightInfo`
  with the result schema and a ticket for data retrieval.
- **`DoGet(ticket)`**: client opens a stream. Server executes the query and
  streams RecordBatches as they are produced — each Parquet page that is
  fetched and verified yields a batch immediately, without waiting for the
  full result set.

```
┌─────────────────────────────────┐
│  LakeSearch Query Service       │
│                                 │
│  :8080  REST/JSON + Arrow IPC   │  ← management API, search, Arrow IPC
│         (axum)                  │
│  :8081  Arrow Flight            │  ← streaming Arrow data via gRPC
│         (tonic + arrow-flight)  │
│                                 │
│  Same query engine underneath   │
└─────────────────────────────────┘
```

Both interfaces call the same query execution pipeline. The difference is
response encoding: JSON, buffered Arrow IPC, or streaming Arrow Flight.

#### DuckDB Integration via RecordBatchReader

DuckDB can query any `pyarrow.RecordBatchReader` directly as a table — and
Arrow Flight `DoGet` returns exactly that. No special DuckDB extension is
needed, just `pyarrow` and `duckdb`:

```python
import duckdb
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8081")
conn = duckdb.connect()

# Text search + aggregation: LakeSearch streams matching rows,
# DuckDB consumes them incrementally for GROUP BY.
reader = client.do_get(flight.Ticket(
    b'{"table":"events","search":{"column":"description","match":"connection timeout"}}'
))
conn.sql("""
    SELECT service, count(*) as cnt, avg(response_time_ms) as avg_rt
    FROM reader
    WHERE timestamp >= '2026-03-01'
    GROUP BY service
    ORDER BY cnt DESC
""").show()

# Early termination: DuckDB stops pulling from the Flight stream
# after 20 rows. LakeSearch stops fetching Parquet pages.
reader = client.do_get(flight.Ticket(
    b'{"table":"events","search":{"column":"description","match":"ECONNREFUSED"}}'
))
conn.sql("SELECT timestamp, service, description FROM reader LIMIT 20").show()

# Join text search results with a local dimension table
reader = client.do_get(flight.Ticket(
    b'{"table":"events","search":{"column":"description","match":"disk space low"}}'
))
conn.sql("""
    SELECT t.team, count(*) as errors
    FROM reader e
    JOIN read_parquet('teams.parquet') t ON e.service = t.service
    GROUP BY t.team
""").show()
```

#### Streaming Behavior and Ranked Queries

Arrow Flight streams RecordBatches incrementally. DuckDB consumes them via
the Arrow C Stream interface, which is pull-based — DuckDB requests the next
batch when its execution engine is ready.

**Unranked queries** (`score: false` or omitted) are fully streaming. The
server emits verified rows as they are produced, page by page. DuckDB can
aggregate, filter, or `LIMIT` incrementally:

- **Aggregation** (`GROUP BY`, `count`, `avg`): DuckDB maintains a hash
  table, processes each batch, discards it.
- **Filter + LIMIT** (without `ORDER BY`): DuckDB stops pulling after enough
  rows. The Flight stream is cancelled, and LakeSearch stops fetching pages.
- **Simple projection**: pure pass-through, no buffering.

**Ranked queries** (`score: true` with `limit`) require the server to
determine the global top-K by BM25 score before streaming. This means:
- The server must evaluate **all** matching pages, score every verified row,
  and maintain a top-K heap internally.
- Only after exhausting all segments does the server stream the final top-K
  rows as RecordBatches.
- This is not incrementally streamable — the server buffers O(K) rows. But
  K is typically small (10-100), so the buffer is small. The latency cost is
  in evaluating all matches, not in buffering.

If the client wants incremental results with scores (e.g., for a live UI),
it can request `score: true` without `limit`. The server streams scored rows
as they are produced, in arbitrary order. The client is responsible for
sorting/truncating.

Queries that must materialize on the DuckDB side (regardless of LakeSearch
streaming):
- **ORDER BY**: DuckDB needs all data to sort.
- **Window functions**: need partition context.
- **Hash join** (Flight result as build side): must buffer to build hash table.

#### Demo: OLAP + Full-Text Search on the Same Dataset

```python
import duckdb
import pyarrow.flight as flight

conn = duckdb.connect()
client = flight.connect("grpc://lakesearch:8081")

# Query 1: Pure OLAP — DuckDB scans Parquet directly
conn.sql("""
    SELECT service, approx_quantile(response_time_ms, 0.99) as p99
    FROM read_parquet('s3://bucket/data/events/*.parquet')
    WHERE timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY service
    ORDER BY p99 DESC
""").show()

# Query 2: Full-text search + OLAP — LakeSearch streams via Flight
reader = client.do_get(flight.Ticket(
    b'{"table":"events","search":{"column":"description","match":"connection timeout"}}'
))
conn.sql("""
    SELECT service, count(*) as timeout_errors, avg(response_time_ms) as avg_rt
    FROM reader
    WHERE timestamp >= now() - INTERVAL 24 HOUR
    GROUP BY service
    ORDER BY timeout_errors DESC
""").show()
```

No data copying. No Elasticsearch. Same Parquet files serve both columnar
analytics and full-text search. DuckDB handles OLAP, LakeSearch handles
text index pruning. Arrow Flight provides Arrow-native transport between
them — no JSON serialization, no schema translation, minimal conversion
overhead. The only dependencies are `pyarrow`, `duckdb`, and a running
LakeSearch query service.

---

## Benchmarking

The index is only useful if it's faster than brute force. We need to
continuously validate that indexed queries outperform full scans across a
range of data sizes, selectivities, and query patterns. The Arrow Flight
integration with DuckDB provides a natural test harness: run the same logical
query both ways and compare.

### Test Harness

A Python benchmark script runs each query in two modes against the same
dataset:

```python
import duckdb
import pyarrow.flight as flight
import time

conn = duckdb.connect()
client = flight.connect("grpc://localhost:8081")
PARQUET_GLOB = "s3://bucket/data/events/*.parquet"

def bench(name, indexed_fn, bruteforce_fn, runs=5):
    """Run both paths, compare wall time and rows scanned."""
    for label, fn in [("indexed", indexed_fn), ("bruteforce", bruteforce_fn)]:
        times = []
        for _ in range(runs):
            start = time.perf_counter()
            result = fn()
            elapsed = time.perf_counter() - start
            times.append(elapsed)
        median = sorted(times)[len(times) // 2]
        print(f"  {name} [{label}]: median={median:.3f}s")

# ── Benchmark 1: Rare term (high selectivity) ──
# Index should win big — prunes almost all files.
def indexed_rare():
    reader = client.do_get(flight.Ticket(
        b'{"table":"events","search":{"column":"description","match":"ECONNREFUSED"}}'
    ))
    return conn.sql("SELECT count(*) FROM reader").fetchone()

# IMPORTANT: brute-force baselines use word-boundary regex, not substring
# ILIKE. Our index does token-based matching, so ILIKE '%conn%' would match
# "disconnect" (substring hit) while the index would not. The regex baseline
# with \b is a practical DuckDB-native approximation — not semantically
# identical to our tokenizer (which does Unicode NFC normalization and has
# its own boundary rules), but close enough for performance comparison.

def bruteforce_rare():
    return conn.sql(f"""
        SELECT count(*) FROM read_parquet('{PARQUET_GLOB}')
        WHERE regexp_matches(lower(description), '\\beconnrefused\\b')
    """).fetchone()

bench("rare_term", indexed_rare, bruteforce_rare)

# ── Benchmark 2: Common term (low selectivity) ──
# Index may not help much — most pages match anyway.
def indexed_common():
    reader = client.do_get(flight.Ticket(
        b'{"table":"events","search":{"column":"description","match":"error"}}'
    ))
    return conn.sql("SELECT count(*) FROM reader").fetchone()

def bruteforce_common():
    return conn.sql(f"""
        SELECT count(*) FROM read_parquet('{PARQUET_GLOB}')
        WHERE regexp_matches(lower(description), '\\berror\\b')
    """).fetchone()

bench("common_term", indexed_common, bruteforce_common)

# ── Benchmark 3: Multi-term AND (high selectivity) ──
# Index intersects posting lists — should prune heavily.
def indexed_multi():
    reader = client.do_get(flight.Ticket(
        b'{"table":"events","search":{"column":"description","match":"connection timeout upstream","operator":"and"}}'
    ))
    return conn.sql("SELECT count(*) FROM reader").fetchone()

def bruteforce_multi():
    return conn.sql(f"""
        SELECT count(*) FROM read_parquet('{PARQUET_GLOB}')
        WHERE regexp_matches(lower(description), '\\bconnection\\b')
          AND regexp_matches(lower(description), '\\btimeout\\b')
          AND regexp_matches(lower(description), '\\bupstream\\b')
    """).fetchone()

bench("multi_term_and", indexed_multi, bruteforce_multi)

# ── Benchmark 4: Prefix search ──
def indexed_prefix():
    reader = client.do_get(flight.Ticket(
        b'{"table":"events","search":{"column":"description","query":{"prefix":"conn"}}}'
    ))
    return conn.sql("SELECT count(*) FROM reader").fetchone()

def bruteforce_prefix():
    return conn.sql(f"""
        SELECT count(*) FROM read_parquet('{PARQUET_GLOB}')
        WHERE regexp_matches(lower(description), '\\bconn\\w*')
    """).fetchone()

bench("prefix_search", indexed_prefix, bruteforce_prefix)

# ── Benchmark 5: Text search + aggregation (end-to-end) ──
def indexed_agg():
    reader = client.do_get(flight.Ticket(
        b'{"table":"events","search":{"column":"description","match":"timeout"}}'
    ))
    return conn.sql("""
        SELECT service, count(*), avg(response_time_ms)
        FROM reader GROUP BY service
    """).fetchall()

def bruteforce_agg():
    return conn.sql(f"""
        SELECT service, count(*), avg(response_time_ms)
        FROM read_parquet('{PARQUET_GLOB}')
        WHERE regexp_matches(lower(description), '\\btimeout\\b')
        GROUP BY service
    """).fetchall()

bench("search_plus_agg", indexed_agg, bruteforce_agg)
```

### What to Measure

| Metric | How | Why |
|--------|-----|-----|
| Wall-clock time | `time.perf_counter()` around each query | Primary metric — is indexed faster? |
| Rows scanned | LakeSearch stats response (`rows_scanned`) vs DuckDB `EXPLAIN ANALYZE` | Validates that the index actually pruned data |
| Files touched | LakeSearch stats (`files_pruned / files_total`) | Shows file-level pruning ratio |
| Bytes read from storage | Object store metrics or `strace` | Confirms I/O reduction, not just CPU |

### Test Matrix

Vary along these axes to find where indexed search wins and where it doesn't:

| Axis | Values |
|------|--------|
| **Data size** | 1GB (100 files), 10GB (1000 files), 100GB (10000 files) |
| **Selectivity** | rare term (0.01% of rows), moderate (1%), common (10%+) |
| **Query type** | single term, multi-term AND, multi-term OR, prefix, suffix |
| **Segment state** | freshly compacted (few large segments) vs fragmented (many small) |
| **Cache state** | warm (FSTs cached) vs cold (empty cache, first query) |

### Expected Outcomes

- **Rare terms / high selectivity**: index should be 10-100x faster. DuckDB
  scans every file; LakeSearch touches 1-2 files.
- **Common terms / low selectivity**: index may be comparable or slower. Most
  pages match, so there's little pruning. The overhead of FST lookup + posting
  list decode + row verification may exceed a simple columnar scan.
- **Multi-term AND**: index should win. Posting list intersection narrows
  candidates multiplicatively.
- **Large data, rare terms**: index advantage grows with data size. Brute
  force is O(data); indexed is O(matches).
- **Cold cache**: first indexed query may be slower (loading FSTs, doc tables).
  Second query onwards should be fast.

### Regression Gate

The benchmark suite should run in CI on representative data. A query where
indexed is slower than brute force by more than 20% is a regression signal —
either the index overhead is too high, or the test case should fall back to
brute force automatically (the query planner could use `doc_frequency` to
estimate selectivity and skip the index for very common terms).

---

## Runtime Model

All three services (indexer, compactor, query) share the same I/O vs CPU
pattern: async object storage reads interleaved with CPU-heavy compute
(tokenization, FST construction/lookup, posting list encode/decode, boolean
evaluation, row verification). Running CPU work directly on tokio's async
worker threads would block the executor and starve I/O tasks.

### Two-Pool Architecture

Each service uses two fixed-size thread pools:

- **Tokio** (async, core-count threads) — I/O: object storage reads/writes,
  HTTP serving, gRPC/Flight streaming, Parquet page fetches via `object_store`.
- **Rayon** (compute, core-count threads) — CPU: FST construction and lookup,
  posting list encoding/decoding/intersection, tokenization, row verification,
  BM25 scoring.

Tokio's `spawn_blocking` is not used for CPU work. Its blocking pool grows
on demand (up to 512 threads by default), which causes excessive context
switching under concurrent CPU load. Rayon's fixed-size work-stealing pool
is designed for compute parallelism — it redistributes work across threads
when one finishes early.

### Shared Abstraction: `LakeRuntime`

A `LakeRuntime` in `lakesearch-core` provides the bridge between tokio and
rayon. All three services use it:

```rust
// lakesearch-core/src/runtime.rs

pub struct LakeRuntime {
    cpu_pool: rayon::ThreadPool,
}

impl LakeRuntime {
    pub fn new(cpu_threads: usize) -> Self {
        Self {
            cpu_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(cpu_threads)
                .thread_name(|i| format!("lakesearch-cpu-{i}"))
                .build()
                .unwrap(),
        }
    }

    /// Run CPU-bound work on the rayon pool, returning the result
    /// to the tokio async context via a oneshot channel. This does
    /// NOT use tokio::spawn_blocking — no tokio blocking threads
    /// are consumed.
    pub async fn cpu<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cpu_pool.spawn(move || {
            let result = f();
            let _ = tx.send(result);
        });
        rx.await.expect("cpu task panicked")
    }
}
```

Usage is the same across all services:

```rust
// Indexer: build FST from terms
let fst_bytes = runtime.cpu(|| build_fst(&terms)).await;

// Compactor: merge posting lists from N segments
let merged = runtime.cpu(|| merge_posting_lists(&segments)).await;

// Query service: intersect posting lists, verify rows
let doc_ids = runtime.cpu(|| intersect_postings(&list_a, &list_b)).await;
let verified = runtime.cpu(move || verify_rows(&batch, &query)).await;
```

### Per-Service Tuning

Each service configures its own thread counts based on workload:

| Service | Tokio Workers | Rayon Threads | Rationale |
|---------|--------------|---------------|-----------|
| Query | core count | core count | Balanced — concurrent queries need both I/O and CPU |
| Indexer | core count / 2 | core count | CPU-heavy (tokenization, FST build). I/O is sequential per batch. |
| Compactor | small (2-4) | core count | Mostly CPU (merge FSTs, posting lists). I/O is bulk sequential read/write. |

---

## Code Organization

### Core Is Pure, Services Orchestrate

`lakesearch-core` is synchronous, pure logic — no I/O, no object storage calls.
It takes bytes in and gives bytes out. The service crates handle all async I/O
and compose core's building blocks via `LakeRuntime`.

```
            ┌─────────────────────────────────────────────┐
            │            Service Crates                    │
            │  (async I/O, orchestration, HTTP/gRPC)       │
            │                                             │
            │  indexer     compactor     query             │
            └──────────────────┬──────────────────────────┘
                               │ depends on
            ┌──────────────────▼──────────────────────────┐
            │            lakesearch-core                    │
            │  (sync, pure, no I/O except LakeRuntime)     │
            │                                             │
            │  types · codecs · algorithms · format        │
            └─────────────────────────────────────────────┘
```

No service crate depends on another service crate. They only share core:

```
     indexer ──────┐
                   │
     compactor ────┼──── lakesearch-core
                   │
     query ────────┘
```

### Core's Public API: Builders and Readers on Bytes

Core never opens a file or makes a network call. Everything works on
`&[u8]` / `Vec<u8>` / `Bytes`:

```rust
// ── Segment writing (indexer and compactor produce these) ──

pub struct SegmentBuilder { .. }
impl SegmentBuilder {
    pub fn new() -> Self;
    pub fn add_file(&mut self, path: &str, row_group_count: u16) -> FileOrdinal;
    pub fn add_page(&mut self, file: FileOrdinal, row_group: u16,
                     page: u16, first_row_index: u64, row_count: u32) -> DocId;
    pub fn add_posting(&mut self, term: &str, doc_id: DocId);
    pub fn set_doc_frequency(&mut self, term: &str, df: u32);
    pub fn set_corpus_stats(&mut self, total_rows: u64, total_tokens: u64);
    pub fn build(self) -> Vec<u8>;  // serialized segment file bytes
}

// ── Segment reading (query service consumes these) ──

pub struct SegmentReader<'a> { .. }
impl<'a> SegmentReader<'a> {
    pub fn open(data: &'a [u8]) -> Result<Self>;
    pub fn doc_table(&self) -> &DocTable;
    pub fn fst(&self) -> &fst::Map<&[u8]>;
    pub fn reverse_fst(&self) -> &fst::Map<&[u8]>;
    pub fn term_info(&self, term_ordinal: u64) -> Result<TermInfo>;
    pub fn read_posting_list(&self, info: &TermInfo) -> Result<Vec<DocId>>;
    pub fn corpus_stats(&self) -> CorpusStats;
}

// ── Posting list codec ──

pub fn encode_posting_list(doc_ids: &[DocId]) -> Vec<u8>;
pub fn decode_posting_list(data: &[u8]) -> Vec<DocId>;

// ── Boolean operations on sorted doc_id arrays ──

pub fn intersect(a: &[DocId], b: &[DocId]) -> Vec<DocId>;
pub fn union(a: &[DocId], b: &[DocId]) -> Vec<DocId>;
pub fn difference(a: &[DocId], b: &[DocId]) -> Vec<DocId>;

// ── Tokenizer ──

pub fn tokenize(text: &str) -> Vec<String>;

// ── BM25 (stateless math) ──

pub fn bm25_score(tf: f32, df: u32, dl: u32, avg_dl: f32, n: u64) -> f32;

// ── Metadata (just serde structs) ──

pub struct Metadata { .. }       // Serialize, Deserialize
pub struct ManifestList { .. }   // Serialize, Deserialize
pub struct Manifest { .. }       // Serialize, Deserialize
pub struct CurrentPointer { .. } // Serialize, Deserialize
```

Core has **no traits**. No `StorageBackend`, no `SegmentStore`. Service crates
call `object_store` directly and hand bytes to core. This avoids leaking async
boundaries and lifetime complexity into the core library.

### Service Crate Patterns

Each service crate follows the same pattern: async I/O at the edges, sync CPU
in the middle via `LakeRuntime.cpu()`.

**Indexer** — read Parquet (async) → tokenize and build segment (sync CPU) →
write segment (async) → CAS commit (async):

```rust
pub async fn index_batch(
    runtime: &LakeRuntime,
    store: &dyn ObjectStore,
    metadata: &Metadata,
    new_files: &[String],
) -> Result<ManifestList> {
    // Async: read Parquet pages from object storage
    let batches = read_parquet_pages(store, new_files).await?;

    // CPU: tokenize rows, build segment (sync, on rayon)
    let (segment_bytes, manifest) = runtime.cpu(move || {
        let mut builder = SegmentBuilder::new();
        for batch in &batches {
            // tokenize each row, call builder.add_posting(), etc.
        }
        (builder.build(), build_manifest(&builder))
    }).await;

    // Async: write segment, manifest, manifest list, commit
    store.put(&segment_path, segment_bytes.into()).await?;
    commit_metadata(store, metadata, &manifest_list).await
}
```

**Query** — load segments (async, cached) → evaluate query (sync CPU) →
read Parquet (async) → verify rows (sync CPU) → yield batches:

```rust
pub fn execute(
    runtime: &LakeRuntime,
    cache: &Cache,
    store: &dyn ObjectStore,
    query: ParsedQuery,
) -> impl Stream<Item = Result<RecordBatch>> {
    async_stream::stream! {
        // Async: load segment bytes (from cache or storage)
        let segments = load_segments(cache, store, &query).await?;

        // CPU: resolve terms → intersect posting lists → candidate doc_ids
        let candidates = runtime.cpu(move || {
            evaluate_boolean(&segments, &query)
        }).await;

        // CPU: doc_ids → row ranges via doc table
        let page_groups = runtime.cpu(move || {
            resolve_to_page_groups(&segments, &candidates)
        }).await;

        // Stream: for each file's pages, async read + sync verify + yield
        for group in page_groups {
            let batches = read_parquet_rows(store, &group).await?;
            for batch in batches {
                let verified = runtime.cpu(move || {
                    verify_and_score(batch, &query, &corpus_stats)
                }).await;
                if verified.num_rows() > 0 {
                    yield Ok(verified);
                }
            }
        }
    }
}
```

**Compactor** — fetch segments (async, concurrent) → merge (sync CPU) →
write merged segment (async) → commit + GC:

```rust
pub async fn compact(
    runtime: &LakeRuntime,
    store: &dyn ObjectStore,
    segments_to_merge: &[SegmentRef],
) -> Result<()> {
    // Async: fetch all segment bytes concurrently
    let segment_bytes = fetch_all_concurrent(store, segments_to_merge).await?;

    // CPU: merge segments (sync, on rayon)
    let merged_bytes = runtime.cpu(move || {
        merge_segments(&segment_bytes)
    }).await;

    // Async: write merged segment, commit metadata, GC sweep
    store.put(&merged_path, merged_bytes.into()).await?;
    commit_metadata(store, metadata, &new_manifest_list).await?;
    gc_sweep(store, metadata).await
}
```

### Data Flow Rules

1. **Bytes flow down**: object storage → service crate → core (for parsing)
2. **Bytes flow up**: core (building) → service crate → object storage
3. **Core never calls async**: service crates bridge via `LakeRuntime.cpu()`
4. **Types flow sideways**: `Metadata`, `DocId`, `TermInfo` etc. defined in
   core, used by all service crates
5. **No service-to-service dependencies**: indexer, compactor, and query
   share only core

---

## Rust Project Structure

```
lakesearch/
├── Cargo.toml
├── crates/
│   ├── lakesearch-core/          # shared types, metadata, segment format, runtime
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── runtime.rs        # LakeRuntime: tokio I/O + rayon CPU pool
│   │   │   ├── metadata.rs       # metadata/manifest/snapshot types + serde
│   │   │   ├── segment.rs        # segment file reader/writer
│   │   │   ├── posting.rs        # posting list encoding/decoding (doc_id based)
│   │   │   ├── doc_table.rs      # doc table: doc_id → (file, rg, page, row range)
│   │   │   ├── tokenizer.rs      # text tokenization
│   │   │   ├── scoring.rs        # BM25 implementation
│   │   │   └── storage.rs        # object storage abstraction (via object_store crate)
│   │   └── Cargo.toml
│   ├── lakesearch-indexer/       # indexing service
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── indexer.rs        # core indexing logic
│   │   │   ├── backfill.rs       # backfill for newly-added columns
│   │   │   └── cas.rs            # compare-and-swap metadata updates
│   │   └── Cargo.toml
│   ├── lakesearch-compactor/     # compaction service
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   └── compactor.rs      # segment merge logic (doc_id remapping)
│   │   └── Cargo.toml
│   └── lakesearch-query/         # query service
│       ├── src/
│       │   ├── main.rs           # REST (axum) + Flight (tonic) servers
│       │   ├── api.rs            # REST request/response types, table management
│       │   ├── flight.rs         # Arrow Flight service implementation
│       │   ├── planner.rs        # query planning (single + multi-column)
│       │   ├── executor.rs       # query execution → Stream<RecordBatch>
│       │   ├── boolean.rs        # AND/OR doc_id list operations
│       │   ├── cache.rs          # LRU cache for FSTs, doc tables, metadata
│       │   └── scorer.rs         # BM25 scoring at query time
│       └── Cargo.toml
└── DESIGN.md
```

### Key Dependencies

| Crate | Purpose |
|-------|---------|
| `fst` | Finite state transducer for term dictionary + prefix/suffix search |
| `parquet` (arrow-rs) | Parquet reading with page-level access + RowSelection |
| `arrow` | Arrow array types for column data |
| `arrow-flight` | Arrow Flight gRPC server for streaming query results |
| `arrow-ipc` | Arrow IPC serialization for HTTP responses |
| `object_store` | Abstraction over S3/GCS/Azure/local filesystem |
| `axum` | HTTP server for REST + management APIs |
| `tonic` | gRPC framework for Arrow Flight server |
| `serde` / `serde_json` | Metadata serialization |
| `lz4_flex` | Block compression for posting lists |
| `uuid` | Unique file naming |
| `tokio` | Async runtime (I/O, HTTP/gRPC serving) |
| `rayon` | CPU thread pool (FST, posting lists, tokenization, scoring) |
| `moka` or `quick_cache` | In-memory LRU cache |

---

## Implementation Phases

Each phase produces a standalone, useful artifact — not just code but
something you can ship and get feedback on.

### Phase 1a: Local Indexing CLI

A command-line tool that indexes local Parquet files and queries them.

- Core library: segment builder/reader, posting codec, FSTs (forward +
  reverse), doc table, tokenizer, boolean ops, BM25 scoring
- Read local Parquet files, build segments, write to local filesystem
- Query from the command line:
  `lakesearch query --table events --match "connection timeout"`
- Single-column, single-segment, returns JSON to stdout
- **Standalone value**: a grep replacement for Parquet files that builds
  a persistent index. Useful immediately for anyone with local Parquet data.

#### Limitations (Phase 1a)

- **Top-level columns only.** The indexed column must be a top-level
  `Utf8` or `LargeUtf8` field. Nested struct fields (e.g.,
  `metadata.title`) are rejected by the Arrow type check. Files with
  structs in *other* columns work fine — column projection uses parquet
  leaf indices, not Arrow field indices, so nested siblings don't
  interfere.

### Phase 1b: Object Storage + Metadata Protocol

The CLI works against S3/GCS/R2 instead of just local files.

- Metadata protocol: `current.json`, metadata files, manifest lists, manifests
- `object_store` integration, LakeRuntime (tokio + rayon)
- CAS commit protocol with retry
- Multiple appends → multiple segments, all scanned at query time
- Batch dedup (`batch_id`)
- **Standalone value**: index Parquet files in your data lake from a laptop,
  query them. No server needed.

### Phase 1c: Query Server

A deployable service with a REST API.

- REST API (axum): JSON and Arrow IPC responses
- Table management API (register tables with fixed columns)
- Query planner + executor as `Stream<RecordBatch>`
- Row verification, BM25 scoring, query timeout
- In-memory cache (FSTs, doc tables, metadata)
- **Standalone value**: a service teams can deploy and query over HTTP.
  First point where multiple users can share an index.

### Phase 1d: Arrow Flight + DuckDB Demo

The "wow" demo — text search + OLAP in one DuckDB session.

- Arrow Flight server (tonic + arrow-flight) alongside REST
- Streaming RecordBatchReader for lazy consumption
- Python demo script: pure OLAP query, text search query, combined query
- **Standalone value**: demonstrates the core thesis — same Parquet files
  serve both columnar analytics and full-text search with no data copying.
  This is the demo that sells the vision.

### Phase 1e: Compaction + GC

Sustained operation without performance degradation.

- Segment merger: merge FSTs, merge posting lists with doc_id remapping,
  carry forward untouched manifests for other columns
- Manifest list compaction
- GC sweep with configurable grace period
- **Standalone value**: the system can run continuously without accumulating
  small segments that slow queries. Required before production use.

### Phase 1f: Benchmarks + Parallel Ingestion

Evidence and scale.

- Benchmark harness (indexed vs brute-force, token-boundary-aware baselines)
- Queue-based parallel ingestion
- I/O optimizations (speculative tail read, batched posting reads, concurrent
  segment loading)
- **Lazy posting list decoding**: instead of fully decoding each posting list
  into `Vec<DocId>` before intersection, use a block-aware cursor that decodes
  blocks on demand. The existing block structure (128 doc_ids per block, with
  `min_doc_id` in each block header) enables skip-ahead: during AND
  intersection, skip blocks whose `min_doc_id` exceeds the current candidate.
  This avoids decoding blocks that can't contribute to the result, reducing
  CPU, temp allocations, and intermediate result sizes — especially impactful
  for multi-term AND queries where one term is common and another is rare.
- **Standalone value**: quantitative proof that indexed search beats brute
  force. Production-ready ingest path for high-volume data.

### Phase 2a: Multi-Column Queries

Search across multiple indexed columns in one query.

- Cross-column row-range intersection via doc table `first_row_index`
- Multi-column BM25 score combination (sum across columns)
- Updated query AST with top-level cross-column AND/OR
- `multi_match` shorthand
- **Standalone value**: search "timeout" in `description` AND "500" in
  `status_code`. Significantly more expressive queries.

### Phase 2b: Dynamic Columns + Backfill

Add or remove indexed columns on a live table.

- Column lifecycle states: `active` / `backfilling` / `dropped`
- Backfill worker: find uncovered files from manifest coverage, index in
  batches, commit incrementally
- Per-file coverage derivation from column-specific manifests
- Brute-force fallback for partially indexed columns (mixed indexed +
  unindexed query path)
- **Standalone value**: add search to a new column without re-ingesting
  the entire table. Makes the system operationally real.

---

## Open Questions

1. **Segment size targets**: What should the target segment size be for compaction?
   Larger segments = fewer files to open during query, but more data to rewrite
   during compaction. Something like 64-256MB per segment seems reasonable.

2. **Delete support**: The current design is append-only. If the underlying data
   lake supports deletes/updates (e.g., Iceberg delete files), how should the
   index handle invalidation? A simple bitmap of deleted doc_ids per segment
   could work.

3. **Cross-segment BM25**: When should we switch from segment-local scoring to
   global stats aggregation? A heuristic based on segment count or variance in
   segment sizes could trigger the global path.

4. **Backfill parallelism**: When adding a column, how many files should the
   indexer process in parallel? This is a resource/throughput tradeoff.

