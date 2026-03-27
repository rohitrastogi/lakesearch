# LakeSearch: External Full-Text Indices for Data Lakes

## 1. Overview

LakeSearch builds external full-text search indices on top of Parquet files stored in
cloud object storage. The indices are "bolted on" — existing query engines (DuckDB, Spark,
DataFusion) remain unaware of them and continue to read the Parquet data normally. LakeSearch
provides its own query path that leverages the indices for keyword and prefix search with
BM25 relevance scoring.

LakeSearch is **library-first**. The query service is the only server. Index, compact, and
vacuum are library functions callable from CLI or user code. All durable state lives in
object storage — no external databases, no local disk.

We require Parquet files to have `offset_index` (page locations and
`first_row_index`). The indexer validates this at ingest time and **rejects**
files that lack it with a clear error. This is a hard requirement — the doc
table's `first_row_index` / `row_count` fields and `RowSelection`-based
page-level reads depend on it. `column_index` (per-page min/max stats) is
not required — it could enable additional pruning in the future but nothing
in the current design depends on it.

### Data Lake Integration

LakeSearch queries the data lake's own metadata at runtime to determine which files
exist, which are new, and which were compacted away. The data lake is the
source of truth for file inventory. LakeSearch never mirrors the file registry.

LakeSearch stores only its own index state (segments, term ranges, last
indexed snapshot per column) in SlateDB — a transactional key-value store
backed entirely by object storage. Segment files also live in object
storage. No external infrastructure beyond the object store and the data
lake's metadata catalog.

### Delete Files

Iceberg supports lazy deletes via position delete files (listing `(file, row)` pairs)
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

For append-only and rewrite-only workloads — which is the primary use case for log/event data —
none of this applies. LakeSearch assumes append-only and rewrite-only data lakes with
no row-level updates, and defers delete support.

### Design Principles

- All durable state in object storage. No external databases, no local disk.
- Query is stateless and read-only. Index/compact mutate metadata.
- Append-only and rewrite-only data lakes. No row-level updates.
- Segment file format, FST, posting lists, BM25 math — all independent of the metadata layer.

### User Contract: Snapshot Retention

LakeSearch stores a per-column `last_indexed_snapshot` referencing a
data lake snapshot. The catalog must be able to resolve this snapshot
(e.g., `files_added_between(last_indexed, target)` needs the old
snapshot's metadata to exist).

**Do not expire data lake snapshots at or older than the most recently
indexed snapshot.** Run `index` before `expire_snapshots` to advance
the pointer past snapshots you intend to expire. If an expired snapshot
is referenced, catalog calls will fail and the index operation must be
retried after resolving the snapshot gap.

### No Time-Travel Queries

LakeSearch does not support querying at historical snapshots. Queries
always run against the current snapshot from the data lake catalog.
Indexes only cover recent data — compact may have cleaned up segments
for files that were compacted away, and there is no mechanism to recover
those results at query time.

For historical queries, use your query engine's native time-travel
(e.g., `SELECT * FROM t AT (VERSION => 5)` in DuckLake/Iceberg/Delta).
LakeSearch's value is fast indexed search on current data, not
historical analysis.

---

## 2. Architecture

### Table Resolution Model

LakeSearch does not assume that every backend uses the same number of
name components. Instead, it separates:

- a **LakeSearch catalog alias** (owned by LakeSearch)
- a **backend-owned table path** (interpreted by the catalog backend)

For server / REST / Flight requests, `table` is a dotted string:

```text
catalog_alias.path.to.table
```

Examples:

- `prod.events`
- `prod.analytics.events`
- `prod.a.b.c`

LakeSearch parses the first segment as the catalog alias. The remaining
segments are passed to the backend implementation, which decides how to
interpret them. For example:

- Iceberg may treat `analytics.events` as namespace `analytics`, table `events`
- Iceberg may treat `a.b.c` as namespace `a.b`, table `c`
- DuckLake may treat `events` as a flat table name
- DuckLake may treat `main.events` as schema `main`, table `events`

For the CLI, the user supplies `--catalog-uri` out of band, so `--table`
is backend-relative and does not need the LakeSearch catalog alias.

### Library-First Deployment Model

The `LakeSearch` struct is the primary API surface. It wraps all
internal infrastructure (SlateDB, catalog, object store, runtime)
behind a single handle. The user points it at their table and calls
methods — no direct interaction with SlateDB, catalogs, or object
store internals.

```rust
// CLI / embedded single-catalog usage: convenience constructor
// resolves a backend-relative table path from a catalog URI.
let ls = LakeSearch::open_from_uri("ducklake:./events.ducklake", &["events"]).await?;
ls.query(query_request).await?;
ls.vacuum(VacuumRequest { grace_period: None }).await?;

// Read-write handle (all operations) — takes writer lock, fences other writers
let ls = LakeSearch::open_mut_from_uri("ducklake:./events.ducklake", &["events"]).await?;
ls.add_index("description", "whitespace_lowercase").await?;
ls.drop_index("description").await?;
ls.index(IndexRequest { target_snapshot: None, target_segment_size: None }).await?;
ls.compact(CompactRequest { target_segment_size: None }).await?;
ls.query(query_request).await?;
ls.vacuum(VacuumRequest { grace_period: None }).await?;

// Query server / multi-catalog usage: resolve lazily from an alias + path
let catalog = server_catalogs.get("prod")?;
let table = catalog.resolve_table(&["analytics".into(), "events".into()]).await?;
let ls = LakeSearch::open(table).await?;
```

`open(table)` and `open_mut(table)` take `Arc<dyn ResolvedTable>` and
open SlateDB in read-only (`DbReader`) or writer (`Db`) mode
respectively. The convenience constructors `open_from_uri` and
`open_mut_from_uri` exist for single-catalog callers such as the CLI:
they construct a backend-specific `Catalog`, resolve the backend-owned
`--table` path, and then call the lower-level `open` / `open_mut`.

Calling `index()` or `compact()` on a read-only handle returns a clear
error.

Internally, `LakeSearch` holds an enum over the two handle types:

```rust
enum MetadataHandle {
    Reader(slatedb::DbReader),
    Writer(slatedb::Db),
}
```

`index()` and `compact()` match on `Writer` and return an error for
`Reader`. `query()` and `vacuum()` work with either — `Db` supports
reads too.

The query server uses lazy `Catalog::resolve_table(...)` + `open`
(read-only) and caches the resulting handles by the full external table
reference. The CLI uses `open_mut_from_uri` for mutating operations and
`open_from_uri` for read-only operations.

`open(table)` uses `slatedb::DbReader::open(...)` without an explicit
checkpoint id. In SlateDB, that means the reader creates and manages its
own checkpoint against the latest persistent manifest, then periodically
polls for manifest / WAL changes at `manifest_poll_interval` and
re-establishes the checkpoint when needed. Each query runs against one
consistent checkpoint from that reader for the duration of the query,
but a long-lived cached read-only handle may lag a recent `index` or
`compact` commit until the next poll. A one-shot CLI invocation opens a
fresh `DbReader`, so it starts from the latest persistent manifest at
open time.

### CLI

Thin wrapper around the library:

```
lakesearch add-index   --catalog-uri ducklake:./events.ducklake --table events --column description [--tokenizer whitespace_lowercase]
lakesearch drop-index  --catalog-uri ducklake:./events.ducklake --table events --column description
lakesearch index        --catalog-uri ducklake:./events.ducklake --table events [--snapshot 5]
lakesearch compact      --catalog-uri ducklake:./events.ducklake --table events
lakesearch vacuum       --catalog-uri ducklake:./events.ducklake --table events [--grace-period 1h]
lakesearch query        --catalog-uri ducklake:./events.ducklake --table events (--json '{...}' | --json-file query.json)
```

For the CLI, `--catalog-uri` selects a single backend catalog for the
entire invocation and `--table` is interpreted relative to that catalog.
The CLI accepts canonical JSON only for the nested query payload; it
does not define a second flag-based query language.

There is no separate `init` command. `add-index` is the initializer:
it resolves the table, creates SlateDB metadata if missing, and writes
the initial `TableConfig`. Later `drop-index`, `index`, `query`,
`compact`, and `vacuum` operate against that initialized metadata. If any non-
initializing verb is called for a resolvable table that does not yet
have LakeSearch metadata, the command returns a clear "run add-index
first" error.

### Server (Optional)

The query service is the only server component. Its config is a catalog
meta file keyed by LakeSearch catalog alias:

```yaml
catalogs:
  prod:
    kind: ducklake
    uri: ducklake:./prod.ducklake
  analytics:
    kind: iceberg
    uri: https://catalog.example.com
```

The server does **not** pre-open tables at startup. Instead, for each
query:

1. Parse the `table` string into `catalog_alias + backend path`
2. Select the configured `Catalog` for that alias
3. Call `resolve_table(...)` on the backend-owned path
4. Open or reuse a cached read-only `LakeSearch` handle for that
   resolved table

This keeps startup light, avoids a static table list, and still allows
hot tables to benefit from caching. The server stays on the read path:
`query` plus optional future read-only metadata endpoints. Mutating
operations remain CLI-only by default.

A long-running server caches:
- Loaded segment files — the main I/O cost. LRU cache by segment path.
- Data lake catalog connections — avoid re-attaching DuckLake on every call.

Whether this is worth it depends on query volume. For occasional CLI use,
no caching needed. For a query service handling many requests, segment
caching is valuable. The library exposes hooks for a caller-provided cache:

```rust
pub trait SegmentCache: Send + Sync {
    async fn get(&self, path: &str) -> Option<Bytes>;
    async fn put(&self, path: &str, data: Bytes);
}
```

Users embedding the library can plug in their own cache strategy.

### Storage Layout

LakeSearch's files live inside the data lake table's own directory:

```
s3://bucket/warehouse/events/              <- the data lake table
  main/products/ducklake-*.parquet         <- data files (managed by data lake)
  lakesearch/                              <- LakeSearch lives here
    segments/
      {column}/{segment_id}.seg
    slatedb/
      manifest/
      wal/
      compacted/
```

Co-located with the table. If you delete the table, the index goes with
it. No separate top-level prefix to manage. Segment IDs use UUIDv7
(time-sortable for debuggability in object storage listings).
One SlateDB instance per table (separate object storage prefix) avoids
single-writer contention across tables.

### Horizontal Scalability

The query service scales horizontally. Index and compact are single-writer
via SlateDB fencing.

```
                    +--------------------+
                    |   Load             |
                    |   Balancer         |
                    +--------+-----------+
                             |
                +------------+------------+
                |            |            |
          +-----v--+   +----v---+   +----v---+
          | Query  |   | Query  |   | Query  |
          |   #1   |   |   #2   |   |   #3   |
          +----+---+   +----+---+   +----+---+
               |            |            |
               +------------+------------+
                            |
                     +------v-------+
                     |   Object     |
                     |   Storage    |
                     |  (S3/GCS/R2) |
                     +--------------+
```

- **Query services** scale with query load. Each instance is a read-only
  process that fetches from object storage and serves results. No
  coordination between instances — they independently read SlateDB via
  `DbReader`, load segments, and execute queries. Per-instance in-memory
  caches (FSTs, doc tables, segment entries) warm up independently; a cold
  instance just makes more object storage reads on its first few queries.
- **Index and compact** are single-writer per table. SlateDB enforces this
  via epoch-based fencing. Running two writers concurrently on the same table
  causes the first to be fenced out — no corruption, just a clear error.

No service holds any durable local state. An instance can be killed and
replaced at any time with no data loss and no coordination. The only cost
of cold-starting a query instance is cache warmup time — optionally
mitigated by preloading FSTs for active segments on startup.

### Two-Pool Architecture (LakeRuntime)

All operations (index, compact, query) share the same I/O vs CPU
pattern: async object storage reads interleaved with CPU-heavy compute
(tokenization, FST construction/lookup, posting list encode/decode, boolean
evaluation, row verification). Running CPU work directly on tokio's async
worker threads would block the executor and starve I/O tasks.

Each operation uses two fixed-size thread pools:

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

#### Shared Abstraction: `LakeRuntime`

A `LakeRuntime` provides the bridge between tokio and rayon:

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

Usage:

```rust
// Indexer: build FST from terms
let fst_bytes = runtime.cpu(|| build_fst(&terms)).await;

// Compactor: merge posting lists from N segments
let merged = runtime.cpu(|| merge_posting_lists(&segments)).await;

// Query service: evaluate terms, resolve candidates, verify rows
let candidates = runtime.cpu(|| evaluate_query(&segments, &query)).await;
let verified = runtime.cpu(move || verify_rows(&batch, &query)).await;
```

#### Thread Pool Defaults

All operations use `LakeRuntime` with core-count defaults for both
tokio (I/O) and rayon (CPU) pools. All three operations — index,
compact, query — use the same producer/consumer pipeline pattern with
concurrent I/O and parallel CPU work. Tune based on profiling, not
assumptions about workload shape.

---

## 3. Segment File Format

The segment file is the core index artifact. It is a binary file designed for:

- Sequential writes (indexer writes it once, never modifies)
- Random-access reads (query service seeks to specific posting lists)
- Block-level compression friendly to object storage range requests

Each segment indexes a single column.

### Design Decisions

#### Dense doc_id space (not inline PageRef tuples)

Each page indexed by a segment is assigned a dense, segment-local `doc_id`
(0, 1, 2, ..., N-1). Posting lists contain sorted entries keyed by `doc_id`.
Each posting entry also carries `row_hit_count: u32` = the number of rows in
that page containing the term at least once. By construction,
`0 < row_hit_count <= row_count` for the referenced page. A separate
**doc table** maps each `doc_id` back to its Parquet location.

We chose this over embedding `(file_id, row_group, page)` tuples directly in
posting lists for several reasons:

- **Compact posting lists.** Each entry stores a dense `doc_id` plus a
  bit-packed `row_hit_count`, which is still substantially more compact and
  compressible than repeating `(file_id, row_group, page)` tuples per hit.
  Posting lists are the largest section of a segment and are read on every
  query, so this matters.
- **Better delta encoding.** Dense doc_ids produce small, regular deltas (often
  just 1), which compress extremely well with varint or bit-packing.
- **Faster intersection.** Intersecting sorted `u32` arrays is a well-studied
  problem with efficient implementations (galloping search, SIMD). Comparing
  multi-field tuples has more overhead.
- **Natural place for page metadata.** The doc table stores `first_row_index`
  and `row_count` per page, which are needed for cross-column intersection.
  Without a doc table, this metadata would need a separate structure anyway.
- **Exact compaction statistics.** `row_hit_count` lives in the posting entry
  because it is term-specific. Compact can sum surviving `row_hit_count`
  values after stale filtering to recompute row-level `doc_frequency`
  exactly, without rereading Parquet.

The tradeoff is one level of indirection: after boolean evaluation produces a
set of `doc_id`s, we look them up in the doc table to get Parquet locations.
The doc table is small (24 bytes per page) and loaded into memory alongside
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
+----------------------------------------------------------+
| Magic: "LKSR" (4 bytes)                                  |
| Version: u16 (2 bytes)                                   |
| Flags: u16 (2 bytes)                                     |
+----------------------------------------------------------+
|                                                          |
|  File Table Section  (variable size, rarely read)        |
|  +----------------------------------------------------+  |
|  | Num files: u32                                     |  |
|  | Per file_ordinal:                                  |  |
|  |   path_offset: u32  (into string pool)             |  |
|  |   path_length: u16                                 |  |
|  |   row_group_count: u16                             |  |
|  | String pool: [u8]                                  |  |
|  +----------------------------------------------------+  |
|                                                          |
+----------------------------------------------------------+
|                                                          |
|  Posting Blocks  (bulk data, read per-term on demand)    |
|  +----------------------------------------------------+  |
|  |   Block 0: [compressed doc_id list]                |  |
|  |   Block 1: [compressed doc_id list]                |  |
|  |   ...                                              |  |
|  +----------------------------------------------------+  |
|                                                          |
+----------------------------------------------------------+
|                                                          |
|  Forward FST Section  (loaded on first access, cached)   |
|  +----------------------------------------------------+  |
|  | FST byte length: u64                               |  |
|  | FST data (built by `fst` crate)                    |  |
|  |   Maps: term (bytes) -> term_ordinal (u64)         |  |
|  |   Used for: exact term lookup, prefix queries      |  |
|  +----------------------------------------------------+  |
|                                                          |
+----------------------------------------------------------+
|                                                          |
|  Reverse FST Section  (loaded on first access, cached)   |
|  +----------------------------------------------------+  |
|  | FST byte length: u64                               |  |
|  | FST data (built by `fst` crate)                    |  |
|  |   Maps: reversed term (bytes) -> term_ordinal (u64)|  |
|  |   Used for: suffix queries                         |  |
|  +----------------------------------------------------+  |
|                                                          |
+---------------------------------------------- TAIL ------+
|  (everything below here fits in one speculative read)    |
|                                                          |
|  Term Info Table  (fixed-width, loaded into memory)      |
|  +----------------------------------------------------+  |
|  | Num terms: u32                                     |  |
|  | Per term_ordinal:                                  |  |
|  |   posting_offset: u64   (into posting blocks)      |  |
|  |   posting_length: u32   (byte length)              |  |
|  |   doc_frequency:  u32   (# rows containing term)   |  |
|  +----------------------------------------------------+  |
|                                                          |
+----------------------------------------------------------+
|                                                          |
|  Doc Table Section  (fixed-width, loaded into memory)    |
|  +----------------------------------------------------+  |
|  | Num docs (pages): u32                              |  |
|  |                                                    |  |
|  | Doc Table (num_docs entries, 24 bytes each):       |  |
|  |   file_ordinal:    u32                             |  |
|  |   row_group:       u16                             |  |
|  |   page_index:      u16                             |  |
|  |   first_row_index: u64                             |  |
|  |   row_count:       u32                             |  |
|  |   token_count:     u32  (for corpus stats in       |  |
|  |                          compaction stale filter)   |  |
|  +----------------------------------------------------+  |
|                                                          |
+----------------------------------------------------------+
|                                                          |
|  Corpus Stats Section  (16 bytes)                        |
|  +----------------------------------------------------+  |
|  | total_rows: u64     (rows across indexed files)    |  |
|  | total_tokens: u64   (total tokens for avg_dl)      |  |
|  +----------------------------------------------------+  |
|                                                          |
+----------------------------------------------------------+
|                                                          |
|  Footer (fixed size: 64 bytes)                           |
|  +----------------------------------------------------+  |
|  | File table section offset: u64                     |  |
|  | Doc table section offset: u64                      |  |
|  | Term info table section offset: u64                |  |
|  | Forward FST section offset: u64                    |  |
|  | Reverse FST section offset: u64                    |  |
|  | Posting lists section offset: u64                  |  |
|  | Corpus stats section offset: u64                   |  |
|  | Segment checksum (CRC32): u32                      |  |
|  | Magic: "LKSR" (4 bytes)                            |  |
|  +----------------------------------------------------+  |
|                                                          |
+----------------------------------------------------------+
```

### Reading a Segment

1. Read the last 64 bytes (footer) to get section offsets
2. Load the doc table into memory (small — 24 bytes x num_pages)
3. Load the forward FST (and optionally the reverse FST) into memory
4. For a query term, look up the forward FST to get `term_ordinal`.
   For a suffix query, reverse the suffix and prefix-search the reverse FST.
5. Read `term_info_table[term_ordinal]` to get posting offset and `doc_frequency`
6. Seek to posting data, decompress and decode the posting entries
   (`doc_id`, `row_hit_count`). Queries usually use only `doc_id`;
   compaction also consumes `row_hit_count`.
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

Each posting list is encoded as a sequence of **blocks** of up to 128 posting
entries:

```
+------------------------------------------+
| Block Header (16 bytes)                  |
|   num_entries: u16                       |
|   min_doc_id: u32     <- skip-ahead      |
|   doc_id_bit_width: u8                   |
|   row_hit_bit_width: u8                  |
|   flags: u8            <- bit 0: LZ4     |
|   reserved: u8                           |
|   compressed_size: u32                   |
|   uncompressed_size: u16                 |
+------------------------------------------+
| Compressed Data                          |
|   delta-encoded doc_id stream            |
|   bit-packed row_hit_count stream        |
|   (optionally LZ4-compressed together)   |
+------------------------------------------+
```

Encoding within a block:
1. Sort posting entries by `doc_id`
2. Delta-encode the `doc_id` array (dense IDs -> small deltas, often 1)
3. Determine `doc_id_bit_width` and `row_hit_bit_width`
4. Bit-pack the delta-encoded `doc_id` stream
5. Bit-pack the `row_hit_count` stream (`u32` logical values)
6. Concatenate the two streams
7. If `flags & 0x01`: apply LZ4 block compression on the concatenated payload.
   `compressed_size` is the LZ4 output size; `uncompressed_size` is needed
   for LZ4 decompression. If LZ4 is not applied, `compressed_size` equals
   the concatenated payload size and `uncompressed_size` is ignored.

The bit-width fields are always stored in the header so the decoder knows
how to unpack both streams regardless of whether LZ4 is applied.

The block structure enables skip-ahead during intersection: read `min_doc_id`
from each block header to decide whether to decompress it.

Queries use `row_hit_count` only indirectly (through the precomputed
`doc_frequency` in the term info table). Compact reads and preserves it
explicitly so stale filtering can rebuild exact row-level `doc_frequency`
without rereading Parquet.

Because `doc_id`s are dense and sorted, delta encoding is highly effective:
most deltas are 1 (consecutive pages) or small integers (sparse hits). This
keeps the location stream compact. `row_hit_count` adds a second small
bit-packed stream, but the posting format is still substantially more
compressible than repeating `(file_id, row_group, page)` tuples per hit.

---

## 4. Tokenization

MVP tokenizer: `whitespace_lowercase`

1. Split on Unicode whitespace and punctuation (`char::is_alphanumeric` boundaries)
2. Lowercase (Unicode-aware)
3. Normalize to NFC
4. Filter tokens shorter than 1 character or longer than 256 bytes
5. Each surviving token becomes a term in the posting list
6. Track term presence per row, then aggregate it into:
   - row-level `doc_frequency` for the term info table
   - per-page `row_hit_count` for each posting entry

Query-time cheap filtering uses the same canonicalization steps, but
does **not** rewrite Parquet data on disk. Instead, LakeSearch builds
temporary Arrow string arrays (or a temporary shadow `RecordBatch`) with
lowercase + NFC applied to the searched columns, runs vectorized
substring `contains` checks on those shadow arrays, combines the masks,
and then filters the original batch before tokenization.

Future tokenizers (not MVP): stemming, n-grams, language-specific.

---

## 5. BM25 Scoring

LakeSearch uses BM25 for relevance ranking. Scoring is computed at the **row level**
at query time, not at the page level.

### Why Row-Level, Not Page-Level

Pages are an I/O access unit — an arbitrary chunk of column values determined by the
Parquet writer's page size settings. They are not semantically meaningful documents.
The actual documents are rows. The index uses pages only for pruning which data to
fetch; scoring must happen after decoding individual rows.

### Scoring Flow

1. **Indexed contribution**: read indexed `doc_frequency(t)` (per term,
   from segment term info tables) and indexed corpus stats
   (`total_rows`, `total_tokens`) from SlateDB.
2. **Unindexed contribution** (only when `score: true` and some searched
   column is behind the current snapshot): brute-force pass 1 over the
   union of unindexed files accumulates exact per-column deltas for the
   columns whose coverage does not yet include each file:
   - `delta_rows`
   - `delta_tokens`
   - `delta_df[t]` = number of rows in the unindexed tail containing
     query term `t`
3. **Finalize BM25 parameters** per searched column from the indexed
   corpus plus any brute-force deltas.
4. **Fetch / verify matching rows** from indexed and brute-force paths.
5. **For each verified row**: compute per-row term frequency (`tf`) and
   score using BM25.

### BM25 Formula

```
score(t, row) = IDF(t) * (tf(t, row) * (k1 + 1)) / (tf(t, row) + k1 * (1 - b + b * dl / avg_dl))
```

Where:
- `tf(t, row)` = term frequency of term `t` in this row (computed at query time
  by tokenizing the row's field value)
- `dl` = document length = total token count of this row's field value
- `avg_dl` = `global_total_tokens / global_N` for this searched column
- `IDF(t) = ln(1 + (N - df(t) + 0.5) / (df(t) + 0.5))`
- `N` = `global_N` = total rows in the full queried corpus for this
  searched column (indexed rows plus any unindexed tail)
- `df(t)` = row-level document frequency for term `t` over that same
  full corpus (see Cross-Segment and Mixed-Coverage Scoring)
- `k1 = 1.2`, `b = 0.75` (standard defaults)

BM25 parameters are computed per searched column. For a row matching
multiple columns, LakeSearch computes one BM25 contribution per column
and sums them.

### Cross-Segment and Mixed-Coverage Scoring

When a query spans multiple segments, or mixes indexed files with an
unindexed tail, BM25 uses **global statistics over the full queried
corpus** for each searched column. There is no segment-local scoring
approximation.

**Indexed contribution**: By the time we score, we've already loaded the
candidate segments needed for posting list evaluation. Each segment's
term info table — loaded in the speculative tail read — contains
row-level `doc_frequency` for each term. Indexed corpus stats
(`total_rows`, `total_tokens`) are already read from SlateDB at query
start.

```
indexed_rows = corpus_stats.total_rows           (from SlateDB)
indexed_tokens = corpus_stats.total_tokens       (from SlateDB)
For each query term t:
  indexed_df[t] = sum(segment.term_info(t).doc_frequency
                      for segment in loaded_segments
                      if segment.fst.contains(t))
```

**Unindexed tail contribution**: If a searched column has unindexed
files and the query requests ranking (`score: true`), brute-force pass 1
tokenizes every row in that column once for the files not yet covered by
that column's snapshot pointer and accumulates:

```
delta_rows += 1 per row in unindexed files
delta_tokens += dl(row)
For each query term t:
  delta_df[t] += 1 if row contains term t at least once
```

After brute-force pass 1 finishes for that column:

```
global_N = indexed_rows + delta_rows
global_total_tokens = indexed_tokens + delta_tokens
global_avg_dl = if global_N > 0 {
  global_total_tokens / global_N
} else {
  0.0
}
For each query term t:
  global_df[t] = indexed_df[t] + delta_df[t]
```

If there is no unindexed tail, then `delta_* = 0` and scoring reduces
to the indexed-only case.

**Zero edge case**: If `global_N == 0` (all segments for a column were
stale-deleted by compact, or column was just added and never indexed),
`avg_dl` is 0 and BM25 scores are 0. The query returns no indexed
results — only brute-force results if unindexed files exist. No
divide-by-zero.

**Execution consequence**: Exact mixed-coverage scoring means ranked
queries cannot finalize BM25 for indexed matches until brute-force pass 1
has contributed `delta_rows`, `delta_tokens`, and `delta_df`. In that
case, indexed matches buffer their projected row plus per-row TF / `dl`
until the final BM25 parameters are known. Filter queries (`score:
false`) remain fully streaming.

**Correctness**: Summing indexed `df(t)` across segments is safe because
no Parquet file appears in two live segments simultaneously. Each index
call creates segments for a specific batch of new files. Compaction
atomically replaces old segments with new merged ones in a single
SlateDB transaction. Term-range split segments cover the same files
but partition the term space — a given term appears in exactly one
sibling, so indexed `df(t)` is never counted twice. Adding `delta_df`
from the unindexed tail is also safe because those files are, by
definition, disjoint from the indexed set.

---

## 6. Parquet Page Access

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
- `OffsetIndexMetaData::page_locations()` -> `Vec<PageLocation>` where each has
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

## 7. Term-Range Splitting and Cross-Segment Intersection

### Why Split

Without splitting, every segment covers the full term vocabulary. A
query for "zebra" loads all segments — even though most of them only
contribute a posting list for that one term. With term-range splitting,
segments cover disjoint term ranges (e.g., "aaa"-"mzz" and "naa"-"zzz").
The query planner only loads segments whose range contains the query
term. For rare terms, this can skip most segments entirely.

Splitting happens during both indexing (when a new segment exceeds
`target_segment_size`) and compaction (when a merged segment exceeds
the target). Each split produces N sibling segments with disjoint term
ranges covering the same set of Parquet files.

### Split Policy

`target_segment_size` defaults to 256MB (total segment file bytes).
Configurable on `CompactRequest` and `IndexRequest`. During the merge
or index build, track output size. When crossing the threshold,
finalize the current segment (its own doc table, FST, dense doc_ids),
start a new one. Each sibling is a fully independent segment — no
shared doc table, no doc_id compatibility constraints. Cross-sibling
AND uses page-location intersection (see below).

### The Cross-Segment AND Problem

Each segment has its own doc_id space — doc_id 7 in seg_A is unrelated
to doc_id 7 in seg_B. For a multi-term AND query where terms land in
different segments (e.g., "connection" in seg_A, "timeout" in seg_B),
you cannot directly intersect their posting lists by doc_id.

### Solution: Page-Location Intersection

When an AND query spans multiple segments, resolve doc_ids to page
locations before intersecting:

```
Cross-segment AND for "connection" AND "timeout":

1. seg_A covers terms "aaa"-"mzz":
     FST lookup "connection" -> posting list -> [doc_id 3, 7, 12]
     Resolve via seg_A doc table:
       3  -> (file1.parquet, rg0, page1)
       7  -> (file1.parquet, rg2, page1)
       12 -> (file2.parquet, rg0, page3)

2. seg_B covers terms "naa"-"zzz":
     FST lookup "timeout" -> posting list -> [doc_id 2, 19]
     Resolve via seg_B doc table:
       2  -> (file1.parquet, rg2, page1)
       19 -> (file3.parquet, rg1, page0)

3. Intersect page locations:
     {(f1,rg0,p1), (f1,rg2,p1), (f2,rg0,p3)} intersect {(f1,rg2,p1), (f3,rg1,p0)}
     = {(f1,rg2,p1)}

4. Read page (f1, rg2, p1), verify row-by-row.
```

**When terms are in the same segment** (unsplit segment, or both terms
in the same split sibling's range), use the fast u32 doc_id intersection
as before. Page-location intersection only applies when terms span
different segments.

### Why This Works

- **No doc_id compatibility constraints.** Each segment has its own
  independent doc_id space. No shared doc tables, no sibling tracking,
  no renumbering rules.
- **No constraints on compaction.** Segments can be merged, split, or
  partially merged freely. The query planner doesn't need to know which
  segments were split from the same parent.
- **Negligible overhead.** Resolving doc_ids through the doc table is a
  fixed-width array lookup (O(1) per doc_id). The extra cost vs. direct
  doc_id intersection is resolving all candidates before intersecting
  rather than just the survivors. For typical posting list sizes
  (hundreds to thousands), this is microseconds. Parquet I/O dominates.

### Query Planner Logic

```
For each query term:
  Find segment(s) whose term range contains the term.
  Look up posting list -> doc_ids.

If all terms resolved from the SAME segment:
  Intersect/union doc_id arrays directly (fast u32 path).
  Resolve survivors via doc table.

If terms come from DIFFERENT segments:
  Resolve all doc_ids via their respective doc tables
    -> sets of (file_path, row_group, page_index).
  Intersect/union the page-location sets (HashSet).

Proceed to page reads and row-level verification.
```

---

## 8. Query Model

### Query API Architecture

The query API has two layers: a **user-facing builder API** that
prioritizes ergonomics, and a **private canonical representation**
(QueryPlan) that the evaluator pattern-matches on. Users never see the
internal representation — builders, REST handlers, and CLI parsers all
produce the same canonical form.

```
User-facing (public):          Internal (private):         Evaluator:
  match_text() ──────┐
  multi_column() ────┼──→  QueryPlan (canonical) ──→  plan / evaluate / verify
  REST JSON ─────────┘ (parsed by handler)
  CLI JSON  ─────────┘ (parsed by clap)
```

#### Library Builders (Rust)

Two entry points cover all query patterns:

```rust
// 90% case: search one column for text (wildcards supported: conn*, *tion)
let req = QueryRequest::match_text("description", "error timeout")
    .operator(BoolOp::And)  // default Or
    .select(&["timestamp", "service"])
    .score(true)
    .limit(10);

// Cross-column predicates: same or different text per column
let req = QueryRequest::multi_column(BoolOp::And)
    .column("description", "error timeout")
    .column("error_message", "timeout reset")
    .limit(10);
```

There is no dedicated `multi_match()` public API. Searching the same
text across multiple columns is expressed explicitly by composing
multiple column clauses with `multi_column()`.

This does not require a separate internal node either. Repeating the
same text across columns compiles to ordinary `ColumnPlan` leaves inside
the canonical query tree.

Builders construct the internal `QueryPlan`. The user never imports
enum variants or constructs AST nodes. Wildcards (`*`) are embedded in
the query text — `parse_query()` tokenizes the text and produces typed
`QueryTerm { Exact, Prefix, Suffix }` values that the evaluator
switches on.

#### CLI

`query` is the subcommand. It accepts the table identity out of band and
the nested query body as canonical JSON via `--json` or `--json-file`
only. This keeps the CLI aligned with the REST / Flight query shape
without inventing a second flag language for nested boolean clauses.

```bash
# single-column match via canonical JSON body
lakesearch query \
  --catalog-uri ducklake:./events.ducklake \
  --table events \
  --json '{
  "search": {
    "column": "description",
    "match": "error timeout",
    "operator": "and"
  },
  "limit": 10
}'

# same text across multiple columns via canonical JSON body
lakesearch query \
  --catalog-uri ducklake:./events.ducklake \
  --table events \
  --json '{
  "search": {
    "or": [
      { "column": "description", "match": "connection refused", "operator": "and" },
      { "column": "error_message", "match": "connection refused", "operator": "and" }
    ]
  },
  "limit": 50
}'

# cross-column bool query via canonical JSON body
lakesearch query \
  --catalog-uri ducklake:./events.ducklake \
  --table events \
  --json '{
  "search": {
    "and": [
      { "column": "description", "match": "error" },
      { "column": "error_message", "match": "timeout" }
    ]
  },
  "limit": 10
}'

# same text across multiple columns
lakesearch query \
  --catalog-uri ducklake:./events.ducklake \
  --table events \
  --json '{
  "search": {
    "or": [
      { "column": "description", "match": "connection refused", "operator": "and" },
      { "column": "error_message", "match": "connection refused", "operator": "and" }
    ]
  },
  "limit": 10
}'

# same, but from a file
lakesearch query \
  --catalog-uri ducklake:./events.ducklake \
  --table events \
  --json-file ./query.json
```

#### REST / Flight

The canonical nested query body is the same `QueryBody` used by the CLI
JSON input and embedded callers. The REST / Flight envelope adds `table`
around that body because the server may be serving multiple catalogs at
once. The REST shapes are intentionally close to Elasticsearch's
`match` / `bool` patterns, but without a separate `multi_match`
surface. Repeating the same `match` clause across columns uses the same
explicit boolean form as any other cross-column query.

For REST / Flight, `table` is the server-side table reference. The
first segment is the LakeSearch catalog alias; the remaining segments
are interpreted by the backend catalog implementation.

#### Internal Canonical Representation (QueryPlan)

Private to the library. The evaluator switches on this — never on raw
text or user-facing types.

```rust
// Private — produced by builders, consumed by planner/evaluator.

enum BoolOp { And, Or }   // used for both within-column and cross-column

struct QueryPlan {
    search: QueryExpr,
    select: Vec<String>,
    limit: Option<usize>,
    score: bool,
    selectivity_threshold: f64,      // default 0.0 (always use index)
}

enum QueryExpr {
    Column(ColumnPlan),
    And(Vec<QueryExpr>),
    Or(Vec<QueryExpr>),
}

struct ColumnPlan {
    column: String,
    terms: Vec<QueryTerm>,           // from parse_query()
    operator: BoolOp,                // within one `match` clause
}

// From tokenizer module (currently lakesearch-core/src/tokenizer.rs;
// moves during crate consolidation in Phase 0)
enum QueryTerm {
    Exact(String),              // FST exact lookup
    Prefix(String),             // FST prefix iteration → union posting lists
    Suffix(String),             // reverse FST prefix iteration → union
}
```

Builders produce this canonical positive-only tree directly. REST /
Flight / CLI parse into the same internal representation.

`QueryTerm` is the atomic unit the evaluator dispatches on:
- `Exact` → single FST lookup → one posting list
- `Prefix` → forward FST prefix iterator → union of posting lists
  (bounded by `MAX_WILDCARD_EXPANSION`)
- `Suffix` → reverse FST prefix iterator → union of posting lists
  within each loaded segment

Wildcard grammar is intentionally small for the MVP:
- `prefix*` → `QueryTerm::Prefix("prefix")`
- `*suffix` → `QueryTerm::Suffix("suffix")`
- `exact` → `QueryTerm::Exact("exact")`

Any raw query atom containing `*` is valid **only** if it has exactly one
`*` and that `*` is the first or last character of the atom. Middle
wildcards, multiple `*` characters, and bare `*` are errors. The wildcard
body must normalize to exactly one token body after lowercase + NFC + token
boundary handling.

If a wildcard expands to more than `MAX_WILDCARD_EXPANSION` (1024)
terms, return an error: "prefix '{prefix}*' expands to N terms,
exceeding limit of 1024." No silent truncation, no brute-force
fallback. The user narrows their query.

Prefix routing uses a half-open lexicographic interval over normalized
term bytes. For prefix `p`, the planner routes to any segment whose
stored term range `[min_term, max_term]` overlaps `[p,
prefix_upper_bound(p))`. `prefix_upper_bound(p)` is the smallest byte
string strictly greater than every byte string beginning with `p`; if no
such upper bound exists, the interval is `[p, +inf)`. Exact routing is a
point lookup on one term, not an interval.

Suffix queries are asymmetric in one important way in the MVP:
reverse-FST lookup works naturally **inside** a segment, but segment
entries are keyed only by forward `min_term` / `max_term`. So exact
queries can prune candidate segments by point lookup and prefix queries
can prune by prefix-interval overlap, while suffix queries load all
segments for the referenced column and rely on reverse-FST expansion
plus normal page/Parquet pruning after that. Reverse-range segment
pruning is future work.

`ColumnPlan.operator` determines how term-level posting lists are
combined within a single `match` clause. `QueryExpr::And` means all
children must match. `QueryExpr::Or` means any child may match. BM25
scores from matching `ColumnPlan` leaves are summed.

LakeSearch's canonical row identity is:
- `(file_path, row_group, row_index_within_row_group)`

`row_index_within_row_group` is the 0-based row ordinal within the
Parquet row group, not within a page, batch, or full file. It is
derived from Parquet `PageLocation.first_row_index` plus the row's
offset within that page. This key is used for deduplication, merging
indexed + brute-force results, and deterministic tie-breaking for equal
BM25 scores.

### Query Language

Queries can search across **multiple indexed columns** with boolean operators.
Queries are recursive boolean expressions over `match` clauses. A
`match` clause always targets exactly one indexed column. `and` / `or`
compose clauses recursively.

The query language provides wildcard support (`conn*` for prefix,
`*tion` for suffix) and a `match` shorthand that mirrors Elasticsearch's
most common query pattern. Wildcards are parsed by `parse_query()` into
typed `QueryTerm` values.

`parse_query()` operates on whitespace-delimited raw atoms first so the
wildcard marker is validated before normal tokenization removes punctuation.
Only single-edge wildcards are supported in the MVP.

The public JSON grammar is:

```text
SearchClause :=
  MatchClause
  | { "and": [ SearchClause, ... ] }
  | { "or":  [ SearchClause, ... ] }

MatchClause :=
  { "column": string, "match": string, "operator"?: "and" | "or" }
```

Validation rules:
- `and` and `or` arrays must be non-empty
- there is no public `term` node; raw text appears only in `match`
- within one whitespace-delimited query atom, `*` is allowed only as a single
  leading or trailing character
- atoms like `con*ion`, `*conn*`, `co*n*`, `*`, or `conn*:refused` are rejected
- wildcard atoms must normalize to exactly one token body; there is no
  escaping, contains-wildcard, regex, or general glob support in the MVP

Negation (`not`) is out of scope for the MVP and reserved for future
work. The current query model is intentionally positive-only.

#### Match (High-Level Shorthand)

The `match` node takes a raw text string, tokenizes it using the column's
configured tokenizer, and combines the resulting terms with an implicit boolean
operator (default: `or`, like Elasticsearch).

```json
{
  "table": "prod.events",
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

When `operator` is `"or"` (or omitted), any token matching is sufficient.
When `operator` is `"and"`, all tokens must match. Wildcards are
supported in the text only as `conn*` (prefix) or `*tion` (suffix).
There is no middle-wildcard, contains, glob, or regex syntax in the
MVP. Unsupported wildcard shapes are rejected rather than silently
reinterpreted.

#### Cross-Column Search

For different queries per column, use the `and` / `or` wrapper:

```json
{
  "table": "prod.events",
  "search": {
    "and": [
      { "column": "description", "match": "error" },
      { "column": "error_message", "match": "timeout" }
    ]
  },
  "limit": 10
}
```

The same shape also handles "same text across multiple columns" by
repeating the same `match` clause explicitly:

```json
{
  "table": "prod.events",
  "select": ["timestamp", "description", "error_message"],
  "search": {
    "or": [
      { "column": "description", "match": "connection refused", "operator": "and" },
      { "column": "error_message", "match": "connection refused", "operator": "and" }
    ]
  },
  "limit": 50
}
```

There is no dedicated `multi_match` shorthand. Same-text searches across
multiple columns use the same explicit composition model as all other
cross-column queries. Per-column BM25 scores are summed for rows that
match in multiple columns.

### Query Response

Responses include query statistics for debugging and understanding index
effectiveness:

The MVP `QueryStats` schema is fixed to these fields:
- `segments_touched`
- `candidate_pages`
- `total_rows_in_snapshot`
- `indexed_rows_scanned`
- `unindexed_rows_scanned`
- `rows_scanned`
- `rows_matched`
- `unindexed_files_scanned`
- `elapsed_ms`

```json
{
  "rows": [ ... ],
  "stats": {
    "segments_touched": 3,
    "candidate_pages": 12,
    "total_rows_in_snapshot": 12000000,
    "indexed_rows_scanned": 240,
    "unindexed_rows_scanned": 120,
    "rows_scanned": 360,
    "rows_matched": 4,
    "unindexed_files_scanned": 0,
    "elapsed_ms": 42
  }
}
```

`total_rows_in_snapshot` is the total row count in the queried lake
snapshot. `rows_scanned` is the actual row-level verification work
performed by the query and is defined as
`indexed_rows_scanned + unindexed_rows_scanned`. The split fields make
it visible how much work came from indexed candidate pages versus the
brute-force unindexed tail.

### Term Resolution

Within one `match` clause, `parse_query()` produces `QueryTerm` values
that the evaluator dispatches on:

- **Exact** (`timeout`): FST exact lookup → one posting list
- **Prefix** (`conn*`): forward FST prefix iteration → union of
  posting lists (bounded by `MAX_WILDCARD_EXPANSION = 1024`)
- **Suffix** (`*tion`): reverse FST prefix iteration → union of
  posting lists inside each loaded segment (bounded)

Segment-level pruning behavior differs by term kind:
- **Exact**: use SlateDB `min_term` / `max_term` to find only
  candidate segments whose range contains that exact term
- **Prefix**: route by overlap with the half-open interval
  `[prefix, prefix_upper_bound(prefix))`; this may match segments whose
  entire term range lies strictly above the raw prefix string itself
- **Suffix**: no segment-level pruning in the MVP; load all segments
  for the column, then use the reverse FST inside each segment

The clause-local `operator` (AND/OR) determines how term-level posting
lists are combined: AND uses sorted-array intersection, OR uses
sorted-array union on `u32` doc_id values.

**Important:** all page-level boolean operations are **approximate**. They
identify candidate pages that *may* contain matching rows, but a page-level
AND does not guarantee every row in the page satisfies the full predicate. The
row-level verification step (see execution pipeline) uses a canonical
shadow-column `contains` pre-filter to cheaply discard most
non-matching rows, then tokenizes only survivors to evaluate the
complete boolean query and eliminate false positives.

#### Negation (Future Work)

Negation (`not`) is intentionally excluded from the MVP query language.
Supporting complement-style predicates correctly and efficiently needs a
separate planner/runtime design, so it is deferred until after the
positive-only query path is stable.

### Multi-Column Intersection

When a query combines searches on different columns with AND, a cross-column
intersection is needed. Different columns have different page boundaries — our
spike confirmed this: with byte-size page limits, `description` (long strings) had
25 pages per row group while `category` (short strings) had 9 and `id` (Int32) had
just 4, all with completely different row ranges.

Cross-column intersection uses the **doc table** and Parquet `offset_index` to
resolve page-level row ranges:

1. For each column's search, evaluate boolean query -> set of `doc_id`s
2. Resolve each `doc_id` through the doc table to get `first_row_index` and
   `row_count`, producing row ranges per column
3. Intersect row ranges across columns — find overlapping intervals
4. Build a `RowSelection` from the overlapping intervals
5. Read matching rows using `ParquetRecordBatchReaderBuilder` with the
   `RowSelection` and a `ProjectionMask` for requested columns

The important staging rule is: resolve to row ranges first, then read
Parquet once for the combined `RowSelection`. Cross-column execution
does not read Parquet separately per segment or per clause.

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

1. **Plan**: Parse query, load segment entries from SlateDB and catalog state,
   identify relevant segments per column. Parse the public JSON / builder
   input into the private `QueryPlan`. Each `match` clause becomes a
   `ColumnPlan` whose `terms` come from `parse_query()`.
2. **Selectivity Estimation**: Estimate selectivity from the query's
   `ColumnPlan` leaves. For each such leaf,
   look up `doc_frequency` from the term info table (one read per term,
   no posting list decoding needed). Compute estimated selectivity:
   - Single term: `df / total_rows`
   - AND: `min(df_a, df_b, ...) / total_rows` (intersection can't exceed
     the smallest posting list)
   - OR: `sum(df_a, df_b, ...) / total_rows` (capped at 1.0)
   - If estimated selectivity exceeds `selectivity_threshold`,
     **skip the index and fall back to brute force scan** for this segment.
     The overhead of decoding posting lists, building RowSelection, and
     verifying rows page-by-page exceeds the cost of a sequential scan
     when most pages match. `selectivity_threshold` is an optional
     per-query parameter (default 0.0 — always use the index). Increase
     once benchmarks show where brute force wins for common terms.
   - For suffix clauses, segment-level selectivity is unknown before the
     relevant segments are loaded, because suffix routing does not use
     forward term ranges. In the MVP, suffix clauses do not benefit from
     segment-pruning selectivity checks.
3. **Term Resolution**: Resolve each term/prefix/suffix node in every
   `ColumnPlan` leaf:
   - Term: forward FST lookup -> one posting list per matching segment
   - Prefix: forward FST prefix iterator -> multiple posting lists -> union
   - Suffix: load all segments for the column, then reverse FST prefix
     iterator (on reversed suffix) -> term_ordinals -> posting lists -> union
4. **Within-Column Candidate Reduction** (approximate): Reduce each
   `ColumnPlan` leaf to candidate row ranges, still without reading
   Parquet data.

   **Fast path** (all terms for the leaf are satisfied inside one
   segment):
   - AND: intersect that segment's `doc_id` lists directly
   - OR: union that segment's `doc_id` lists directly
   - Resolve surviving `doc_id`s through that segment's doc table

   **General path** (the leaf spans multiple segments, including
   term-range split siblings):
   - Resolve each term's hits through the owning segments' doc tables
     to page locations `(file, row_group, page)`
   - AND: intersect page-location sets
   - OR: union page-location sets
   - Convert surviving page locations to row ranges via
     `first_row_index` + `row_count`

   Output: candidate row ranges per `ColumnPlan` leaf. No Parquet pages
   are read yet.
5. **Cross-Clause Row-Range Reduction**: Combine leaf row ranges
   according to the enclosing `QueryExpr` tree. Cross-column AND/OR and
   same-column AND/OR across split siblings both operate here on row
   identity / overlapping row ranges, before any Parquet fetch.
6. **File Grouping**: Group candidate row ranges by `(file, row_group)`,
   deduplicate, and sort by `first_row_index` within each group. This
   produces one work item per `(file, row_group)` pair. Grouping enables
   coalesced reads per file instead of scattered per-page reads.
7. **RowSelection Construction**: For each `(file, row_group)` work item,
   build a `RowSelection` from its sorted row ranges. Adjacent or
   overlapping ranges are merged. The `RowSelection` tells the Parquet
   reader exactly which rows to decode and which to skip.
8. **Page Fetch**: read candidate rows once per work item using
   `ParquetRecordBatchReaderBuilder` with `RowSelection` +
   `ProjectionMask` (include all searched columns)
9. **Row-Level Verification** (canonical shadow-column pre-filter +
   tokenization):
   Page-level posting lists are approximate — a candidate page with 8192
   rows may contain only a few actual matches. To avoid tokenizing every
   row (which dominates CPU for large pages or long text fields), apply
   a two-stage filter:

   a. **Canonical shadow-column pre-filter**: Build a temporary Arrow
      string array (or temporary `RecordBatch`) for each searched column
      in the current batch with lowercase + NFC applied. For each query
      term body, run vectorized substring `contains` checks on that
      shadow column, AND/OR the resulting boolean masks across terms,
      and filter the original batch with the combined mask. This cheaply
      discards the vast majority of non-matching rows while preserving
      the tokenizer's canonicalization semantics.
   b. **Tokenize survivors only**: For rows passing the shadow-column filter,
      tokenize and evaluate the compiled `QueryPlan`. Discard rows that
      don't match. The `contains` pre-filter is still a superset filter
      (substring, not token-boundary), so a few false positives
      reach this stage, but the count is small.
      Implementations should benchmark this against direct
      tokenize-every-row verification and keep the pre-filter only where
      it wins on the target workload.

   This is the same canonical shadow-column pre-filter used in the
   brute-force path, applied to the indexed path's verification step.
   For a page where 3 out of 8192 rows match, tokenization runs on
   ~5-10 rows (shadow-column survivors) instead of 8192.
10. **Unindexed Tail + BM25 Finalization**:
    - **Filter mode** (`score: false` or omitted): brute-force the
      unindexed tail with the shadow-column pre-filter +
      tokenize-survivors
      path, then emit verified rows immediately. No TF computation, no
      IDF lookup, no BM25 math. If `limit` is specified, stop after N
      verified rows (no ranking — first-N, not top-N). Fully streamable.
    - **Search mode** (`score: true`): if every referenced column is
      fully indexed, score indexed rows immediately after verification
      using indexed global `df` + corpus stats.
    - If any referenced column has unindexed files, brute-force pass 1
      tokenizes every row in the unindexed tail for those columns once,
      accumulates exact `delta_rows`, `delta_tokens`, and `delta_df`
      for the query terms on a per-column basis, and records matching
      rows plus per-row TF / `dl`. Indexed matches buffer the same
      per-row information until those deltas are known. Then finalize
      BM25 parameters from the indexed corpus plus deltas, and score
      both indexed and brute-force matches with the same full-corpus
      statistics.
11. **Projection / Output**: return only the requested `select` columns
    (with scores in search mode). If `limit` is specified in search
    mode, maintain a top-K heap and emit the top-K by score after all
    candidates are evaluated. Without `limit`, scored rows may stream in
    arbitrary order once BM25 parameters are finalized.

### Brute Force Fallback

Used when indices are unavailable (column has not been fully indexed yet or
some files are not covered). The query path identifies unindexed files by comparing
the column's `last_indexed_snapshot` against the current table snapshot via
`table.files_added_between(last_indexed, current_snapshot)`.

For indexed files: use the indexed path (page-level candidates -> row
verification).

For un-indexed files, the brute-force path uses two-pass projection. In
multi-column queries, the brute-force file set is the union across the
referenced columns' `unindexed_files`; BM25 deltas are still tracked per
column, and only columns that do not yet cover a given file contribute
`delta_rows`, `delta_tokens`, or `delta_df` for that file.

1. **Pass 1 — stream only the searched columns**: Read full row groups
   sequentially (large I/O, not page-random).
   - **Filter queries** (`score: false`): use the canonical
     shadow-column pre-filter (temporary lowercase + NFC Arrow arrays
     plus vectorized substring `contains`) to cheaply discard
     non-candidate rows, then tokenize survivors and evaluate the
     compiled `QueryPlan`. Record matching row indices.
   - **Ranked queries** (`score: true`): tokenize every row in the
     searched columns once. This pass simultaneously:
     - evaluates the compiled `QueryPlan`
     - records matching row indices
     - records per-match TF / `dl`
     - accumulates exact per-column `delta_rows`, `delta_tokens`, and
       `delta_df` for the query terms
2. **Finalize BM25 parameters** (ranked queries only): combine indexed
   corpus stats / `df` with the brute-force deltas from pass 1.
3. **Pass 2 — read remaining projected columns for matches only**: Build
   `RowSelection` from matching row indices. Read other columns with
   `ProjectionMask` + `RowSelection`. Score matches with the finalized
   BM25 parameters.

Two-pass avoids reading non-searched columns for the ~99% of rows
that don't match. Sequential column streaming is also cheaper on
object stores than many small page reads (fewer requests, better
throughput). Ranked mixed-coverage queries are slower than fully
indexed queries because pass 1 must compute exact BM25 corpus deltas
for the unindexed tail, but the resulting scores are exact.

Merge and deduplicate results from both indexed and brute-force paths.

---

## 9. Arrow Data Interface

The query service exposes two interfaces:

- **REST (JSON)**: Simple search endpoint for top-K queries and
  lightweight integrations. Materializes results, returns JSON.
- **Arrow Flight (gRPC)**: Streaming Arrow data for OLAP engine
  integration (DuckDB, DataFusion, Polars). Zero-copy, incremental.

```
+-------------------------------------------+
|  LakeSearch Query Service                  |
|                                            |
|  :8080  REST/JSON                          |  <- search, JSON responses
|         (axum)                             |
|  :8081  Arrow Flight                       |  <- streaming Arrow data via gRPC
|         (tonic + arrow-flight)             |
|                                            |
|  Same query engine underneath              |
+-------------------------------------------+
```

Both interfaces call the same query execution pipeline.

### Arrow Flight (gRPC)

For streaming large result sets and native SQL integration with DuckDB, the
query service exposes an Arrow Flight endpoint. The Flight protocol maps
to our query pipeline:

- **`GetFlightInfo(command)`**: client sends a search query as the command
  payload. Server parses the query, plans execution, and returns `FlightInfo`
  with the result schema and a ticket for data retrieval.
- **`DoGet(ticket)`**: client opens a stream. Server executes the query and
  streams RecordBatches as they are produced — each Parquet page that is
  fetched and verified yields a batch immediately, without waiting for the
  full result set.

#### Flight Ticket Schema

The ticket is the search request as UTF-8 JSON bytes. Same fields as
the REST `SearchRequest` in `api_types.rs`. Normative field names:

| Field | Type | Required | Default |
|-------|------|----------|---------|
| `table` | string | yes | — |
| `search` | object | yes | — |
| `search.column` | string | yes (match) | — |
| `search.match` | string | yes (match) | — |
| `search.operator` | string | no | `"or"` |
| `search.and` / `search.or` | object[] | yes (bool composition) | — |
| `select` | string[] | no | all columns |
| `limit` | integer | no | unlimited |
| `score` | boolean | no | false |
| `selectivity_threshold` | float | no | 0.0 |

`table` is the server-side external table reference. It is encoded as a
dotted string where the first segment is the LakeSearch catalog alias
and the remaining segments are interpreted by that backend. Examples:
`prod.events`, `prod.analytics.events`, `warehouse.a.b.c`.

Example:
```
b'{"table":"prod.events","search":{"column":"description","match":"error timeout","operator":"and"},"select":["timestamp","service"],"limit":10,"score":true}'
```

Agents implementing Flight client/server MUST use these exact field
names. The REST handler and Flight handler parse the same JSON shape.
The CLI supplies `--catalog-uri` and `--table` separately, then builds
the same internal `QueryBody` before calling the library.

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
    b'{"table":"prod.events","search":{"column":"description","match":"connection timeout"}}'
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
    b'{"table":"prod.events","search":{"column":"description","match":"ECONNREFUSED"}}'
))
conn.sql("SELECT timestamp, service, description FROM reader LIMIT 20").show()

# Join text search results with a local dimension table
reader = client.do_get(flight.Ticket(
    b'{"table":"prod.events","search":{"column":"description","match":"disk space low"}}'
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
as they are produced, in arbitrary order, **when all referenced columns are
fully indexed**. If a ranked query includes an unindexed tail, LakeSearch must
first finish brute-force pass 1 for that tail so BM25 can finalize `N`,
`avg_dl`, and `df`. The client is responsible for sorting/truncating.

#### Query Cancellation

When the client drops the Flight stream or REST connection, bounded
channel drops propagate backward through the pipeline — I/O producers
stop reading Parquet pages, in-flight rayon tasks complete but results
are dropped on send to a closed channel. No explicit cancellation
token needed. Query timeouts are deferred to the production polish
phase.

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
    b'{"table":"prod.events","search":{"column":"description","match":"connection timeout"}}'
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

## 10. Catalog Resolution and Table Metadata

LakeSearch delegates file inventory to the data lake's own catalog. It
never discovers tables or Parquet files by listing object storage. The
catalog resolves a backend-relative table path into a table-bound handle,
and that handle exposes the snapshot/file primitives LakeSearch needs.

### Table References

At the API boundary, a server-side table reference is:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub catalog_alias: String,
    pub object_path: Vec<String>,
}
```

`catalog_alias` belongs to LakeSearch. It selects one configured backend
catalog from the server's catalog meta file. `object_path` belongs to the
backend implementation. LakeSearch does not assume a universal 3-part or
4-part identifier:

- Iceberg may interpret `["analytics", "events"]` as namespace
  `analytics`, table `events`
- Iceberg may interpret `["a", "b", "c"]` as namespace `a.b`, table `c`
- DuckLake may interpret `["events"]` as a flat table name
- DuckLake may interpret `["main", "events"]` as schema `main`, table `events`

For CLI usage, `--catalog-uri` already pins the catalog, so `--table`
only needs to provide the backend-relative `object_path`.

### Catalog and Resolved Table Interfaces

```rust
/// A snapshot / version identifier in the data lake.
///
/// LakeSearch requires all supported backends to expose a non-negative,
/// totally ordered 64-bit integer identifier for snapshot progress.
/// This makes the monotonic backfill rules mechanically implementable:
/// `SnapshotId` values are compared numerically, never lexicographically.
///
/// Examples:
/// - DuckLake: BIGINT `snapshot_id`
/// - Iceberg: `snapshot-id` (long)
/// - Delta Lake: table version / commit version
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId(pub i64);

/// A data file tracked by the data lake.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataFile {
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
}

/// Backend-specific catalog, already selected by LakeSearch catalog alias.
///
/// Each implementation owns the interpretation of `object_path`.
#[async_trait]
pub trait Catalog: Send + Sync {
    async fn resolve_table(
        &self,
        object_path: &[String],
    ) -> Result<Arc<dyn ResolvedTable>>;
}

/// Table-bound metadata handle returned by `Catalog::resolve_table(...)`.
///
/// This is the object LakeSearch keeps around after lazy resolution.
#[async_trait]
pub trait ResolvedTable: Send + Sync {
    fn table_location(&self) -> &str;

    async fn current_snapshot(&self) -> Result<SnapshotId>;

    async fn files_at_snapshot(
        &self,
        snapshot: &SnapshotId,
    ) -> Result<Vec<DataFile>>;

    async fn files_added_between(
        &self,
        from: &SnapshotId,
        to: &SnapshotId,
    ) -> Result<Vec<DataFile>>;

    async fn check_files_live(
        &self,
        paths: &[String],
    ) -> Result<HashSet<String>>;
}
```

There is no separate `CatalogRegistry` trait. The server can simply load
its catalog meta file into a `HashMap<String, Arc<dyn Catalog>>` keyed by
catalog alias. The CLI's `open_from_uri` / `open_mut_from_uri` helpers
construct one backend-specific `Catalog` directly from `--catalog-uri` and
resolve `--table` against it.

There is also no need for an internal `ResolvedTableBackend` enum.
Backend-specific resolved table types implement the `ResolvedTable`
trait directly:

- `DuckLakeCatalog::resolve_table(...)` returns `Arc<DuckLakeResolvedTable>`
- `IcebergCatalog::resolve_table(...)` returns `Arc<IcebergResolvedTable>`
- `DeltaCatalog::resolve_table(...)` returns `Arc<DeltaResolvedTable>`

Because LakeSearch stores one SlateDB instance per resolved table, the
metadata keyspace is table-local. The resolved table handle supplies the
runtime table location, snapshot, and file inventory primitives; it does
not need to expose a separate public table identifier for keying.

### Cross-Format Compatibility

The resolved-table interface is intentionally minimal. No format-specific
concepts (DuckLake's `begin_snapshot` / `end_snapshot`, Iceberg manifest
entry status, Delta Add / Remove actions) leak into the rest of LakeSearch.

| Operation | DuckLake | Iceberg | Delta Lake |
|-----------|----------|---------|------------|
| `resolve_table(path)` | Lookup table metadata in DuckLake catalog tables, return `Arc<DuckLakeResolvedTable>` | Resolve namespace + table through Iceberg catalog, return `Arc<IcebergResolvedTable>` | Resolve table from `_delta_log` root / catalog metadata, return `Arc<DeltaResolvedTable>` |
| `current_snapshot()` | `SELECT snapshot_id FROM ducklake_snapshots(...) ORDER BY snapshot_id DESC LIMIT 1` | `table_metadata.current_snapshot_id` | latest table version |
| `files_at_snapshot(S)` | `WHERE begin_snapshot <= S AND (end_snapshot > S OR IS NULL)` | Read manifest list + manifests at snapshot S, filter live data files | load version S + enumerate active data files |
| `files_added_between(A, B)` | `WHERE begin_snapshot > A AND begin_snapshot <= B` | Walk snapshot chain A->B, collect `ADDED` manifest entries | Read commit entries A+1..B, collect Add actions |
| `check_files_live(paths)` | `WHERE path IN (...) AND active` — targeted SQL | Stream manifests, check against input set — same I/O, O(paths) memory | Stream log/checkpoints, check against input set |

**Stale file detection**: compact uses `files_at_snapshot(current)` to
build the full live set (batch operation, memory is acceptable). Query
uses `check_files_live(candidate_paths)` to check only the files from
candidate segments, avoiding a full catalog materialization on the hot
path.

**Why no `files_removed_between()`**: all major formats can expose
removals, but the semantics differ. Iceberg `DELETED` entries can refer
to delete files, not only compaction. Delta Remove actions carry a
`data_change` flag. Rather than overfitting the interface to those
differences, LakeSearch derives liveness from `files_at_snapshot` and
`check_files_live`, which are portable and sufficient.

### LakeSearch's Diff Logic (Not the Catalog's Job)

The catalog resolves tables and exposes file listing primitives.
LakeSearch owns the business logic:

- **New files to index**: `files_added_between(last_indexed, target)`
- **Stale files to clean up**: files referenced by existing segments
  whose paths are not in `files_at_snapshot(current_snapshot)`

This keeps backend integrations thin and lets LakeSearch test diff and
cleanup behavior independently of any specific catalog format.

### Implementation Notes Per Format

**DuckLake** (via the `duckdb` crate): all methods are local SQL queries
over DuckLake metadata tables. This is the simplest implementation and
the best fit for local development and CLI usage.

**Iceberg** (via `iceberg-rust` crate or REST catalog): resolving the
table is cheap, but file inventory comes from Avro manifests. Caching is
important:

1. Cache manifests by path (immutable once written)
2. Cache the current manifest list until the snapshot changes
3. On snapshot advance, diff manifest lists and read only new manifests

With this, steady-state `check_files_live` and `files_added_between`
become in-memory scans over cached manifests.

**Delta Lake** (via `deltalake` / delta-rs): resolve the table from the
transaction-log root, then read JSON commits and checkpoints from
`_delta_log/`.

### DuckLake Implementation (First Target)

DuckLake stores file metadata in `ducklake_data_file` with
`begin_snapshot` / `end_snapshot` columns, which map directly to what
LakeSearch needs.

```rust
pub struct DuckLakeCatalog {
    conn: Mutex<duckdb::Connection>,
    catalog_name: String,
}
```

`duckdb::Connection` is `!Send`, so the connection is guarded by a
`Mutex`. This is acceptable because catalog calls are tiny local SQL
queries; the lock is held only for the duration of each metadata lookup,
not during Parquet reads or segment processing.

`DuckLakeCatalog::resolve_table(...)` is responsible for:

1. interpreting the backend-relative path (`events`, `main.events`, etc.)
2. looking up the DuckLake `table_id`
3. discovering the table's object-storage location
4. returning a `DuckLakeResolvedTable` implementing the `ResolvedTable` trait

**Setup**:

For LakeSearch, the DuckLake catalog URI is the DuckLake metadata
storage location (for example `ducklake:./events.ducklake`). This
design assumes LakeSearch is connecting to an **existing** DuckLake.
In that case DuckLake loads the data storage location from the catalog
database, so LakeSearch does not need a separate user-facing
`DATA_PATH` parameter.

`DATA_PATH` is only required when creating a new DuckLake outside
LakeSearch. A necessary precondition for LakeSearch is that the
warehouse / lake already exists.

```rust
impl DuckLakeCatalog {
    pub fn new(ducklake_path: &str) -> Result<Self> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
        conn.execute_batch(&format!("ATTACH 'ducklake:{}' AS lake;", ducklake_path))?;
        conn.execute_batch(
            "CALL lake.ducklake_set_option('lake', 'inline_data', false);"
        )?;
        Ok(Self { conn: Mutex::new(conn), catalog_name: "lake".into() })
    }
}
```

**Resolved-table query implementations**:

```sql
-- current_snapshot():
SELECT snapshot_id FROM ducklake_snapshots('lake')
ORDER BY snapshot_id DESC LIMIT 1;

-- files_at_snapshot(snapshot_id):
SELECT path, file_size_bytes, record_count
FROM __ducklake_metadata_lake.ducklake_data_file
WHERE table_id = ?
  AND begin_snapshot <= ?
  AND (end_snapshot > ? OR end_snapshot IS NULL);

-- files_added_between(from, to):
SELECT path, file_size_bytes, record_count
FROM __ducklake_metadata_lake.ducklake_data_file
WHERE table_id = ?
  AND begin_snapshot > ?
  AND begin_snapshot <= ?;

-- check_files_live(paths):
SELECT path
FROM __ducklake_metadata_lake.ducklake_data_file
WHERE table_id = ?
  AND path IN (?, ?, ...)
  AND end_snapshot IS NULL;
```

### DuckLake Deployment Caveat

DuckLake's metadata catalog is a single DuckDB file (for example
`./events.ducklake`). This is excellent for local and CLI-oriented
workflows where LakeSearch is co-located with the writer process.

It is a poor fit for distributed query-service deployments because the
query server would need access to that same mutable DuckDB file. DuckDB
files cannot be safely shared via object storage. For production server
deployments, prefer Iceberg or Delta Lake backends whose metadata is
already accessible remotely.

### DuckDB-RS Usage

The `duckdb` Rust crate with the `bundled` feature compiles DuckDB from
source. All LakeSearch needs is the ability to submit SQL:

```rust
use duckdb::{params, Connection};

let conn = Connection::open_in_memory()?;
conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
conn.execute_batch("ATTACH 'ducklake:./catalog.ducklake' AS lake;")?;

let mut stmt = conn.prepare(
    "SELECT path, file_size_bytes, record_count
     FROM __ducklake_metadata_lake.ducklake_data_file
     WHERE table_id = ? AND begin_snapshot > ? AND begin_snapshot <= ?"
)?;
let files: Vec<DataFile> = stmt.query_map(params![table_id, from, to], |row| {
    Ok(DataFile {
        path: row.get(0)?,
        size_bytes: row.get(1)?,
        row_count: row.get(2)?,
    })
})?.collect::<Result<_, _>>()?;
```

### Disabling Inlined Data

DuckLake can inline small writes into the metadata database instead of
writing Parquet files. LakeSearch cannot index data that never lands in
Parquet, so this must be disabled on catalog setup:

```sql
CALL ducklake_set_option('lake', 'inline_data', false);
```

### Querying File Metadata

DuckLake exposes its catalog tables via `__ducklake_metadata_{catalog}`.
The key table is `ducklake_data_file`:

| Column | Type | Description |
|--------|------|-------------|
| `data_file_id` | BIGINT | PK |
| `table_id` | BIGINT | Which table |
| `begin_snapshot` | BIGINT | File became active at this snapshot |
| `end_snapshot` | BIGINT | File retired at this snapshot (NULL = active) |
| `path` | VARCHAR | Object storage path |
| `record_count` | BIGINT | Row count |
| `file_size_bytes` | BIGINT | File size |

This is enough to answer all LakeSearch metadata questions:

- **Files active now**: `end_snapshot IS NULL` (or `> current_snap`)
- **Files added since X**: `begin_snapshot > X`
- **Files live among a candidate set**: `path IN (...) AND end_snapshot IS NULL`

### What Compaction Looks Like in DuckLake

When a user calls `ducklake_merge_adjacent_files('lake')`:

1. old small files get `end_snapshot` set to the new snapshot
2. new merged files get `begin_snapshot` set to the new snapshot
3. merged files may span multiple prior snapshots

From LakeSearch's perspective:

- **index** sees the merged files as newly added files
- **compact** sees the retired input files as stale files that should be
  removed from segment references

This is exactly the append-and-rewrite pattern the design assumes.

---

## 11. SlateDB Metadata Store

### Why SlateDB

- **Object-storage-native**: SSTables, WAL, and manifest all live in the
  same object store as LakeSearch segments. No external infrastructure.
- **Transactional**: Snapshot isolation and SSI for atomic metadata updates.
- **Range queries**: Sorted keyspace with `scan()` and `scan_prefix()` —
  ideal for term-range lookups on segments.
- **Single binary**: Embedded library, no server process.
- **Single flat keyspace**: No tables or column families. We use key
  prefixes to partition logical concerns. A single transaction can
  atomically update keys across all prefixes — strictly simpler than
  multi-table transactions.

### Why Not DuckDB-as-Metadata-Store?

DuckDB files are mutable. Object stores don't support in-place mutation,
so every metadata write would rewrite the entire `.duckdb` file. SlateDB
is designed for object storage: it writes small immutable SSTable files
and coordinates via an append-only manifest with CAS. Writes are
incremental, not full-file rewrites.

### Key Layout

All keys are byte strings. `\x00` is used as the separator byte in the
actual implementation (can't appear in valid UTF-8 strings, sorts before
all other bytes). `|` is used in this doc for readability. Values are
bincode-encoded structs.

```
GLOBAL STATE
  meta|config                         -> TableConfig
    Contains:
      indexed_columns: Vec<ColumnConfig>
        Each: { name: String, tokenizer: String, status: active|dropped }
        MVP invariant: at most one `ColumnConfig` per column name.
        Multiple indexes / tokenizers per column are out of scope.

  meta|snapshot|{column}              -> ColumnSnapshotState
    Contains:
      last_indexed_snapshot: SnapshotId
    Per-column tracking. Each column advances independently.
    A newly added column has no entry (equivalent to last_indexed = None).
    In the happy path (all columns in sync), all entries have the same
    snapshot value and advance in lockstep.

  meta|corpus|{column}                -> CorpusStats
    Contains:
      total_rows: u64
      total_tokens: u64
    Per-column global stats for BM25. Updated atomically during index
    and compact.

SEGMENT INDEX
  seg|{column}|{max_term}|{segment_id} -> SegmentEntry
    Contains:
      segment_path: String
      min_term: String
      max_term: String             (redundant with key, for convenience)
      size_bytes: u64
      doc_count: u64               (page-level doc_ids)
      total_rows: u64
      total_tokens: u64
      parquet_files: Vec<ParquetFileRef>
        Each: { path: String, row_group_count: u32 }

    Sorted by (column, max_term). A range scan starting at
    seg|{col}|{query_term} finds all segments whose term
    range may contain the query term.
```

**Why this works for queries**:
- **Exact term `T`**: scan from `seg|{col}|{T}` forward. The first key
  with `max_term >= T` is the first candidate. Keep scanning until
  `min_term > T`.
- **Prefix `P*`**: scan from `seg|{col}|{P}` forward. Keep segments whose
  `[min_term, max_term]` overlaps `[P, prefix_upper_bound(P))`, and stop
  once `min_term >= prefix_upper_bound(P)` (or never, if the upper bound
  is unbounded).

This is O(matching segments), not O(all segments).

**Why this works for atomic updates**: A single SlateDB transaction can
put/delete keys across all prefixes (meta, seg, corpus). No multi-table
transaction needed.

### Initialization

```rust
let store = object_store::aws::AmazonS3Builder::new()
    .with_bucket_name("my-bucket")
    .with_region("us-east-1")
    .build()?;

let db = slatedb::Db::builder(
    Path::from("warehouse/events/lakesearch/slatedb"),
    Arc::new(store),
).build().await?;
```

### Commit Model

Writes go to in-memory WAL -> flushed to object storage as WAL SSTs ->
compacted into sorted runs. Manifest updated via CAS.

After committing index/compact metadata, call `db.flush().await` to
ensure durability before returning success. Without flush, a crash could
lose the update (segment files would be orphaned, cleaned by vacuum).

### Publish Contract

Segment files are written to object storage BEFORE the SlateDB
transaction commits. If the transaction fails, segment files become
orphans (cleaned by vacuum). If segment upload fails, the transaction
never starts. There is no window where metadata references a
non-existent segment file.

Each query uses one consistent SlateDB read snapshot — either fully
before or fully after a transaction. All segment entries from one
index/compact commit appear atomically. A query that started before a
commit continues seeing the old segment set for its entire execution.

After compact commits, old segment keys are deleted from SlateDB but
old segment files remain on object storage until vacuum runs. Readers
that loaded old entries before the commit can still read the old files.

**Invariant: segment files always outlive their metadata entries.**

### Segment Lifecycle

```
Written:          File on object storage, no SlateDB entry yet.
                  (In-flight index/compact, pre-commit.)
Live:             File on storage AND SlateDB entry references it.
                  (Queries use this segment.)
Replaced:         SlateDB entry deleted (by compact), file still exists.
                  (Old readers may still be mid-query on it.)
Vacuum-eligible:  Replaced AND file age > grace_period.
Deleted:          File removed by vacuum.
```

Grace period (default 1h) must exceed the maximum expected query
execution time.

### Writer Fencing

SlateDB enforces single-writer via epoch-based fencing. If two processes
try to write concurrently, the second fences out the first. This provides
coordination for index and compact operations without requiring external
locking.

### Read Path (Query and Vacuum)

Query and vacuum use `DbReader` for read-only access without writer
contention:
```rust
let reader = slatedb::DbReader::open(path, Arc::new(store)).await?;
let entry = reader.get(key).await?;
let iter = reader.scan_prefix(prefix).await?;
```

Multiple concurrent query and vacuum processes supported.

LakeSearch opens `DbReader` without an explicit checkpoint id. Per
SlateDB's checkpoint model, such a reader establishes a checkpoint on
the latest persistent manifest, uses that checkpoint consistently for
reads, and polls again at `manifest_poll_interval` to decide whether to
re-establish it. If an explicit checkpoint id were supplied, the reader
would stay pinned to that checkpoint and would not poll for new state.
Therefore:
- each query sees one consistent SlateDB checkpoint for its full
  planning / execution lifetime
- a cached read-only handle eventually observes new `index` / `compact`
  commits after the reader's next poll, not necessarily immediately at
  query start
- MVP query planning re-reads active segment entries from SlateDB each
  query rather than caching mutable segment-entry results across queries

### Concurrency Model

**Writer concurrency**: SlateDB enforces single-writer per database
via epoch-based fencing. If a second process opens the same SlateDB
path for writing, it increments the epoch and the first process's next
SlateDB operation fails fast with a clear fencing error. No silent
corruption, no hanging — just an immediate error. Any segment files
the fenced process wrote but didn't commit become orphans, cleaned by
vacuum.

This is reasonable behavior. The user sees a clear error ("another
process is writing to this table") and knows what to do. No special
detection logic needed — SlateDB's fencing gives us the right
semantics out of the box.

**Recommended usage patterns**:

- **CLI**: Run index, compact, vacuum sequentially. If you accidentally
  run two commands on the same table, the first one gets fenced and
  fails fast. No harm done.
- **Daemon**: One process per table, operations serialized:
  ```
  loop:
    index(table)
    compact(table)
    // vacuum on a slower cadence
    sleep(poll_interval)
  ```
- **Multiple tables**: Parallel across tables using separate SlateDB
  instances. No shared state between tables.

**Reader concurrency**: Query and vacuum only read SlateDB (via
`DbReader`). They can run concurrently with each other and with a
writer. Each query sees a consistent point-in-time metadata snapshot —
either before or after a metadata commit, never partial state.
Vacuum's grace period (default 1 hour) prevents it from deleting
segment files that a concurrent index/compact has written but not yet
committed.

---

## 12. API Specifications

**Table resolution**:
- CLI commands take `--catalog-uri` and a backend-relative `--table`.
- REST / Flight / server requests carry `table` as
  `catalog_alias + backend path`.
- The pseudocode below assumes `table` is a resolved `ResolvedTable`
  handle and reads or writes table-local SlateDB keys inside that
  table's own SlateDB instance.

**Initialization**:
- `add-index` is the initializer. It creates SlateDB metadata if missing
  and records the initial `TableConfig`.
- `index`, `query`, `compact`, `vacuum`, and `drop-index` require that
  metadata to already exist. If it does not, they return a clear
  "run add-index first" error.
- MVP supports only one index definition per column. `add-index` on an
  already-active column returns an error. To change tokenizer, run
  `drop-index`, then `add-index`, then `index`. Multiple indexes per
  column and planner-side index combination are future work.

**Snapshot handling**: Index accepts an optional `target_snapshot` for
incremental backfill. Per-column snapshot pointers are monotonic: an
index call may advance a lagging column to the requested target, but it
never moves any column backward. All other APIs — query, compact,
vacuum — always operate on the current snapshot. There is no time-travel
query support. LakeSearch indexes only work on recent data; for
historical queries, use your query engine's native time-travel.

### 12a. Index API

**Purpose**: Index data files added to the data lake since the last
index operation. Each column tracks its own progress independently.
In the happy path (all columns in sync), one catalog diff advances
all columns in lockstep. When a new column is added, it starts with
`last_indexed = None` and catches up by indexing all existing files.

```rust
pub struct IndexRequest {
    /// Snapshot to index up to. If None, uses current snapshot.
    pub target_snapshot: Option<SnapshotId>,
    /// Split segments exceeding this size. Default 256MB.
    pub target_segment_size: Option<u64>,
}

pub struct IndexResult {
    pub segments_created: usize,
    pub files_indexed_per_column: HashMap<String, usize>,
    pub target_snapshot: SnapshotId,
}
```

**Algorithm**:

```
INDEX(table, target_snapshot?):

1. RESOLVE target snapshot:
     target = target_snapshot.unwrap_or(table.current_snapshot())

2. READ table config from SlateDB for indexed columns.
     Filter to columns with status = active.

3. For each active column, READ its snapshot pointer:
     key: meta|snapshot|{column}
     If not found -> last_indexed = None (first index or new column).

4. CLASSIFY columns relative to target:
     behind_or_uninitialized = columns where last_indexed is None or < target
     at_target               = columns where last_indexed == target
     ahead_of_target         = columns where last_indexed > target

     If behind_or_uninitialized is empty AND ahead_of_target is non-empty:
       ERROR: target snapshot is older than every active column pointer.
       Index never rewinds a column to an older snapshot.

5. COMPUTE per-column file sets for behind_or_uninitialized columns:
     Group those columns by their last_indexed value (most will be the same).
     For each distinct last_indexed value:
       If last_indexed is None:
         files = table.files_at_snapshot(target)
       Else:
         files = table.files_added_between(last_indexed, target)

     This avoids redundant catalog calls — columns that are in sync
     share one call. Columns at_target do no work. Columns ahead_of_target
     are left unchanged.

6. For each column that has new files:
     a. BUILD segment(s) from that column's file set:
        - Read each Parquet file's column pages via offset_index
        - Tokenize every row, build posting lists
          (`doc_id`, `row_hit_count`)
        - Build doc table: doc_id -> (file, row_group, page, first_row, count)
        - Compute per-segment corpus stats (total_rows, total_tokens)
        - Build FST term dictionary
        - If segment > target_segment_size: split by term range

     b. WRITE segment file(s) to object storage:
        {table.table_location()}/lakesearch/segments/{column}/{segment_id}.seg

7. COMMIT atomically (single SlateDB transaction):
     txn = db.begin(Snapshot)
     For each column with new segments:
       For each segment:
         txn.put(seg_key(col, seg.max_term, seg.id), encode(seg))
       // Update corpus stats
       old_stats = txn.get(corpus_key(col))
       txn.put(corpus_key(col), old_stats + seg_stats)
     // Advance only columns that were behind target or uninitialized.
     // Columns already at or ahead of target keep their existing pointer.
     For each active column:
       If old_snapshot is None or old_snapshot < target:
         txn.put(snapshot_key(col), target)
     txn.commit()
     db.flush()   // ensure durable before returning

8. Return IndexResult { ... }
```

**Pointers are monotonic.** Index never rewrites a column to an older
snapshot. If `target` is older than every active column pointer, fail.
If some columns lag and others are already ahead, index backfills only
the lagging columns to `target` and leaves the ahead columns unchanged.
In the common case where all columns start aligned and `target` is
newer, they advance together.

**Backfill is not a separate operation.** When a new column is added
(see Column Lifecycle), its snapshot pointer doesn't exist. The next `index` call sees
`last_indexed = None` for that column, fetches all files via
`files_at_snapshot(target)`, and indexes them. Existing columns that
are already at or ahead of target do nothing. If the existing columns
were already at `target`, then after the call all columns are in sync.

**Missing columns**: If a Parquet file does not contain the indexed
column (e.g., the column was added to the schema after the file was
written), the indexer skips that file for that column. No doc table
entries are created for it. At query time, brute-force scanning of that
file finds no matches for the missing column, which is correct.

**Incremental backfill** for very large tables: The caller can control
the pace by passing explicit `target_snapshot` values. Instead of
indexing all the way to the current snapshot in one call, index up to
snapshot 5, then 10, then 15. The `target_snapshot` parameter already
exists for this — no additional API needed. Orchestrating the cadence
is the caller's responsibility.

**Idempotency**: Calling with the same target snapshot is a no-op.
Calling with an older target is allowed only when at least one active
column is behind or uninitialized; in that case, the call backfills only
those lagging columns and never rewinds ahead columns. Calling with a
target older than every active column pointer fails.

**Failure modes**:
- Segment write fails before commit -> no metadata change. Orphan files
  cleaned by vacuum.
- SlateDB commit fails -> segments written but unreferenced. Vacuum cleans
  up. Safe to retry.

---

### 12b. Compact API

**Purpose**: Two responsibilities unified into a single pass:
1. **Stale segment cleanup**: Detect Parquet files that have been compacted
   away by the data lake and filter them out during merge.
2. **Size-tiered segment merge**: Merge small segments into larger ones.

Compact does NOT take a target snapshot. It queries the current snapshot
at invocation time to get the freshest liveness information.

Calling index before compact is not required for correctness — compact
will correctly identify stale files regardless. But calling index first
means the per-column snapshots are more recent, which reduces the number
of segments that need stale-file filtering at query time.

```rust
pub struct CompactRequest {
    /// Split segments exceeding this size. Default 256MB.
    pub target_segment_size: Option<u64>,
}

pub struct CompactResult {
    pub stale_segments_cleaned: usize,
    pub segments_merged: usize,
    pub segments_created: usize,
}
```

**Algorithm** — unified plan-then-execute:

Stale cleanup and size-tiered merging happen in a single pass. The
planner builds merge groups that combine both concerns, and the executor
reads input segments, merge-sorts by term while filtering out stale
doc_ids, and writes final segments once. No intermediate segment files,
no double I/O.

```
COMPACT(table):

--- Plan -----------------------------------------------------------

1. QUERY table for current liveness state:
     current_snap = table.current_snapshot()
     live_files = table.files_at_snapshot(current_snap)
     live_paths = { f.path for f in live_files }

2. SCAN all segments for this table from SlateDB:
     scan_prefix(seg|)

3. ANNOTATE each segment with staleness:
     For each segment:
       stale_files = { f for f in segment.parquet_files if f.path NOT IN live_paths }
       If stale_files == segment.parquet_files -> fully_stale (delete, no merge needed)
       Else if stale_files is non-empty -> partially_stale (must be included in a merge group)
       Else -> clean

4. BUILD merge groups per indexed column:
     a. Remove fully_stale segments (they'll just be deleted).
     b. Group remaining segments by size tier:
          Tier 0:  0 - 1 MB       (fresh ingest)
          Tier 1:  1 - 10 MB
          Tier 2:  10 - 100 MB
          Tier 3:  100 MB - 1 GB   (target steady-state)
          Tier 4:  1 GB+           (never merge unless stale)
     c. A tier becomes a merge group if:
          - It has >= min_merge_count segments (default 4), OR
          - It contains ANY partially_stale segment
        The second condition ensures stale segments always get
        rewritten, even in a tier with few segments. A stale segment
        alone in its tier becomes a single-segment merge group (i.e.,
        a rewrite-in-place with stale filtering).
     d. Tier 4 segments are normally excluded from merging. But if a
        Tier 4 segment is partially_stale, it forms its own merge
        group for rewriting.

--- Execute --------------------------------------------------------

5. For each merge group:
     a. READ all segment files in the group from object storage.

     b. MERGE with stale filtering:
        Merge-sort all segments by term. For each term:
          - Union posting entries across segments.
          - For each `(doc_id, row_hit_count)` in the unioned list,
            look up the doc table. If the doc table entry points to a
            stale Parquet file -> discard. Otherwise -> remap to a new
            dense doc_id and carry `row_hit_count` forward.
          - If posting list is empty after filtering -> drop the term.
          - Recompute `doc_frequency` as the sum of surviving
            `row_hit_count` values.
        Concatenate surviving doc table entries (renumbered densely).
        Compute corpus stats from surviving entries.

     c. If merged result > target_segment_size:
        SPLIT by term range into N segments with disjoint
        [min_term, max_term].

     d. WRITE new segment file(s) to object storage.

6. COMMIT atomically (single SlateDB transaction):
     txn = db.begin(Snapshot)
     // Delete fully_stale segments
     For each fully_stale segment:
       txn.delete(seg_key(col, old_max_term, old_seg_id))
     // Replace merged segments with new ones
     For each merge group:
       For each consumed segment: txn.delete(old_key)
       For each new output segment: txn.put(new_key, entry)
     // Update global corpus stats per column
     For each affected column:
       old_corpus = txn.get(corpus_key(col))
       new_corpus = old_corpus - sum(consumed_segment_stats)
                               + sum(new_segment_stats)
       txn.put(corpus_key(col), new_corpus)
     txn.commit()
     db.flush()

7. Return CompactResult { ... }
```

**Why one pass**: Two separate passes (rewrite stale segments, then
merge small segments) would produce intermediate segment files that get
immediately consumed by the merge pass — wasted I/O. The unified
approach reads each input segment once and writes each output segment
once. The stale filtering is just an additional predicate during the
merge: "skip doc table entries for stale Parquet files."

**Why compact queries current snapshot, not last_indexed**: The point of
liveness analysis is to know which Parquet files still exist in the data
lake right now. The freshest information comes from the current snapshot,
not from whenever we last indexed. This makes compact correct regardless
of whether index was called first.

**Merge primitive with stale filtering**: The existing merge primitive is
extended with a `stale_paths` parameter. Stale filtering is just an
additional predicate during the merge: "skip doc table entries for stale
Parquet files." Because each posting entry also carries `row_hit_count`,
the merge can recompute exact row-level `doc_frequency` as
`sum(row_hit_count)` over surviving entries for each term. This
naturally handles terms that disappear (posting list becomes empty),
doc_id renumbering, exact `doc_frequency` recomputation, corpus stats
recomputation, and FST rebuild — all of which the merge already does.
A single-segment merge group (a stale segment alone in its tier) is a
degenerate case: same code path, no special case.

```rust
/// Merge N segments into one (or more, if splitting by term range).
/// Entries referencing any path in `stale_paths` are discarded.
fn merge_segments(
    segments: &[Segment],
    stale_paths: &HashSet<String>,
    target_segment_size: u64,
) -> Result<Vec<Segment>>;
```

**Compact does not advance `last_indexed_snapshot`.** Only index advances
it. The snapshot pointer strictly tracks "all files up to this snapshot
have been indexed for this column."

---

### 12c. Query Path

The query API, query language (match and explicit boolean composition),
cross-column semantics, BM25 scoring, and execution pipeline are
defined in the Query Model section. The changes below describe how the query
*path* determines which files are indexed, which are unindexed, and which
are stale:

- **Per-column snapshot pointer**: Read from
  `meta|snapshot|{column}` in SlateDB. Each column may be
  at a different snapshot (e.g., during backfill).
- **Unindexed file detection**: Uses `table.files_added_between(
  last_indexed, current_snapshot)` to identify files not yet indexed.
- **Stale page filtering**: Uses `table.check_files_live(paths)`
  with only the candidate segments' file paths. Avoids materializing
  the full file list on the query hot path.

**Algorithm**:

```
QUERY(table, query):

--- Step 1: Determine indexed vs unindexed files -------------------

1. GET current snapshot:
     snap = table.current_snapshot()

2. For each referenced column, READ that column's snapshot pointer from SlateDB:
     key: meta|snapshot|{column}
     If not found -> last_indexed = None (column never indexed).

3. Determine unindexed files per referenced column:
     If last_indexed == snap:
       unindexed_files = []
     Else if last_indexed is None:
       unindexed_files = table.files_at_snapshot(snap)  // everything
     Else:
       unindexed_files = table.files_added_between(last_indexed, snap)

4. Stale filtering is deferred to after segment discovery (step 7).
     Always collect parquet file paths from candidate segments after
     step 6, then call table.check_files_live(paths) to get the live
     subset. This avoids materializing the full file list — only the
     files from candidate segments are checked.

--- Step 2: Indexed path (candidate generation; no Parquet reads) ---

5. PARSE the request into `QueryPlan`.

6. For each query term / column pair in the `QueryPlan`:
     If term is Exact:
       SCAN SlateDB for candidate segments:
         Start: seg|{column}|{term}
         End:   seg|{column}|~
         Keep segments where min_term <= term <= max_term.
     If term is Prefix:
       SCAN SlateDB for candidate segments:
         Start: seg|{column}|{prefix}
         End:   seg|{column}|~
         Keep segments whose [min_term, max_term] overlaps
           [prefix, prefix_upper_bound(prefix)).
         Stop once min_term >= prefix_upper_bound(prefix)
           (or never, if upper bound is unbounded).
     If term is Suffix:
       use all segments for that column
       (MVP: no reverse-range segment pruning metadata)

     Within one query, coalesce repeated same-column segment-discovery
     work in a query-local scratch map. If multiple clauses need nearby
     or identical discovery ranges for the same column, the planner may
     issue one broader SlateDB scan and partition / filter the returned
     segment entries in memory for those clauses. If two branches still
     request the same discovery result, compute it once and reuse it for
     the rest of the query.

7. LOAD the candidate segments (from cache or object storage).
     call table.check_files_live(candidate_segment_paths) once
     to get live_paths.

     Within one query, also single-flight already-open immutable segment
     artifacts (FST, doc table, posting blocks) by segment path so
     repeated clause evaluation does not re-open or re-decode the same
     segment twice.

8. For each `ColumnPlan` leaf:
     a. Resolve each referenced term in each matching segment:
          FST lookup -> term ordinal -> posting list offset -> decode postings
     b. If all terms for the leaf can be satisfied inside one segment:
          INTERSECT / UNION that segment's `doc_id` lists directly.
          Resolve surviving `doc_id`s through that segment's doc table.
     c. Otherwise:
          Resolve each term's postings through the owning segments'
          doc tables to page locations `(file, row_group, page)`.
          INTERSECT / UNION those page-location sets.
     d. FILTER out any page whose Parquet file path is not in
          live_paths.
     e. Convert surviving page hits to row ranges via
          `(first_row_index, row_count)`.

9. Combine the per-leaf row ranges according to the enclosing
     `QueryExpr` tree. Cross-column AND/OR and same-column AND/OR
     across split siblings both happen here on canonical row identity
     `(file_path, row_group, row_index_within_row_group)` /
     overlapping row ranges. No Parquet data has been read yet.

10. Group the resulting row ranges by `(file, row_group)` and build
      one `RowSelection` work item per group.

11. For each indexed work item:
      READ candidate rows once with `RowSelection + ProjectionMask`.
      Build temporary lowercase + NFC shadow Arrow arrays for the
        searched columns, run vectorized substring `contains` per query
        term, combine masks, and filter the original batch.
      Tokenize only shadow-column survivors, evaluate the compiled
        `QueryPlan`.
      If `score: true` and every referenced column is fully indexed:
        score immediately with indexed global df + corpus stats.
      Else if `score: true`:
        buffer the verified indexed rows plus per-row TF / `dl` for
        final scoring after the unindexed tail contributes its deltas.

--- Step 3: Unindexed path (brute-force) ---------------------------

12. For each file in the union of `unindexed_files` across referenced
      columns (two-pass projection):
      Pass 1 — stream only the searched columns:
        If `score: false`:
          build temporary lowercase + NFC shadow Arrow arrays, apply
          vectorized substring `contains`, tokenize survivors, evaluate
          `QueryPlan`, record matching row indices.
        Else (`score: true`):
          tokenize every row once, evaluate `QueryPlan`, record
          matching row indices + per-row TF / `dl`, and accumulate
          exact per-column `delta_rows`, `delta_tokens`, and
          `delta_df` for the query terms only for columns whose
          snapshot pointer does not yet cover that file.
      Pass 2 — read remaining projected columns for matches only:
        Build `RowSelection` from matching row indices.
        Read other columns with `ProjectionMask + RowSelection`.

13. If `score: true` and any unindexed files were scanned:
      finalize per-column BM25 parameters from indexed corpus stats +
      brute-force deltas, then score buffered indexed matches and
      brute-force matches with those finalized parameters.

--- Step 4: Merge and return ---------------------------------------

14. MERGE indexed results + brute-force results.
15. If `score: true`, SORT by BM25 descending and apply `limit`.
    If `score: false`, preserve streaming/encounter order and apply
    `limit` without a global sort.
16. Return results with stats.
```

For cross-column queries (designed in § 8, not yet implemented in the
query crate), the implementation should follow this same staged plan:
reduce each `ColumnPlan` leaf to row ranges first, combine row ranges
according to the enclosing `QueryExpr` tree, then read Parquet once for
the combined `RowSelection` work items.

**Query during backfill**: If a column is mid-backfill (its
`last_indexed` is behind other columns), queries on that column
brute-force scan more files. This is correct — results are complete,
just slower until the backfill catches up. `unindexed_files_scanned`
in QueryStats tells the caller how much brute-force work was needed.

**Stale page filtering (step 8d)**: Between index and compact, some
segments reference Parquet files the data lake has already compacted.
Step 8d filters these at query time. Running compact promptly reduces
this overhead by rewriting the segments themselves.

**Performance tiers**:
- Best: fully indexed, no staleness -> no brute-force; only targeted
  candidate-path liveness checks.
- Typical: small unindexed tail -> most results from index, small scan.
- Worst: nothing indexed -> full brute-force. Same as no index at all.

**Optimization for the query path stale check**: Query never
materializes the full live file set. It always performs a targeted
`check_files_live(candidate_paths)` on only the paths referenced by the
candidate segments. Keeping the index up to date still helps by reducing
the unindexed tail, but it does not eliminate this targeted liveness
check.

---

### 12d. Vacuum API

**Purpose**: Delete orphaned segment files from object storage. These
accumulate from failed index/compact operations or replaced segments.

```rust
pub struct VacuumRequest {
    /// Only delete files older than this duration. Default: 1 hour.
    pub grace_period: Option<Duration>,
}

pub struct VacuumResult {
    pub files_deleted: usize,
}
```

**Algorithm**:

```
VACUUM(table, grace_period = 1h):

1. LIST all segment files in object storage:
     {table.table_location()}/lakesearch/segments/
   This is the only API that uses object storage LIST.
   Run sparingly (daily or weekly).

2. SCAN SlateDB for all live segment paths:
     scan_prefix(seg|)
     live_paths = { entry.segment_path for each entry }

3. orphans = listed_paths - live_paths

4. For each orphan:
     Check last-modified timestamp.
     If older than grace_period -> DELETE from object storage.
     (Grace period prevents deleting in-flight segments from a
      concurrent index/compact that hasn't committed yet.
      UUIDv7 segment IDs guarantee that a fenced indexer's
      orphan files have different paths than a subsequent indexer's
      live files — no collision possible, so vacuum cannot
      accidentally delete a live segment.)

5. Return VacuumResult { files_deleted }
```

**Vacuum only handles LakeSearch segment files.** SlateDB manages its
own garbage collection internally — old SSTables in
`{table.table_location()}/lakesearch/slatedb/compacted/` are cleaned up by
SlateDB's built-in compactor. Vacuum does not touch the `slatedb/`
directory.

---

## 13. Column Lifecycle

### Adding an Index

```bash
lakesearch add-index \
  --catalog-uri ducklake:./events.ducklake \
  --table events \
  --column description \
  [--tokenizer whitespace_lowercase]
```

If the table has not been initialized yet, `add-index` first creates the
SlateDB metadata for the resolved table and writes the initial
`TableConfig`. It then adds the column (status = active). It does NOT
create a snapshot pointer for the column — the absence of a pointer means
`last_indexed = None`.

MVP supports only one index definition per column. If the column is
already active in `TableConfig`, `add-index` returns an error. If the
column exists in `TableConfig` with status = `dropped`, `add-index`
reactivates that same column entry with the supplied tokenizer and leaves
the snapshot pointer absent so the next `index` call performs a full
backfill. Supporting multiple active indexes per column is future work.

The next `index` call sees the new column with no snapshot pointer,
fetches all files via `files_at_snapshot(target)`, and indexes them.
After the call, the new column is in sync with the others. This is
backfill — not a separate operation, just the natural behavior of
per-column snapshot tracking.

### Dropping an Index

```bash
lakesearch drop-index \
  --catalog-uri ducklake:./events.ducklake \
  --table events \
  --column description
```

Single atomic SlateDB transaction:
1. Update `TableConfig`: set column status to `dropped`
2. Delete all segment keys for that column:
   `scan_prefix(seg|{column}|)` -> delete each
3. Delete corpus stats: `delete(meta|corpus|{column})`
4. Delete snapshot pointer: `delete(meta|snapshot|{column})`

Segment files for the dropped column become orphans, cleaned up by the
next `vacuum` run. The index API ignores columns with status = dropped.

### Column Status

Only two states: `active` and `dropped`. There is no `backfilling`
state. A column is "backfilling" if its snapshot pointer is behind the
others — this is observable from the metadata but doesn't need a
separate status. The index API handles it naturally.

---

## 14. Caching

The query service maintains an in-memory cache to avoid repeated object storage
reads. Cached items and their staleness strategy:

### What to Cache

| Item | Size | TTL / Invalidation Strategy |
|------|------|-----------------------------|
| FSTs (from segment files) | Medium (typically KB-low MB per segment) | Immutable. Cache keyed by segment path. Evict via LRU. Highest-value cache items. |
| Doc tables (from segment files) | Small-medium (24 bytes x pages per segment) | Immutable. Cache keyed by segment path. Needed for every query that hits this segment. |
| Parquet footer + offset_index | Small | Immutable per file. Cache keyed by file path. Needed only for cross-column queries if doc table doesn't already have `first_row_index` (it does). |
| Posting list blocks | Large | Immutable. Optional — only cache hot blocks under LRU. |

### Staleness Safety

Segment files are **immutable files**. Once written, they never change. This means
cache invalidation is trivial:

- Each query reads the active segment entries fresh from SlateDB via the
  current `DbReader` checkpoint. LakeSearch does **not** cache these
  mutable metadata reads across queries.
- When a new segment entry appears (after index or compact commits),
  later queries may start loading that segment's immutable data into the
  object cache.
- When a SlateDB read shows a segment is no longer referenced, it can be evicted
  from the immutable object caches by ordinary LRU pressure. The
  underlying file will be cleaned up by vacuum.

The only race condition: a cached segment file could be vacuumed while still in cache.
Mitigate by ensuring vacuum's grace period is much longer than max query execution time.

### Cache Implementation

Use `moka` LRU cache with a configurable memory budget (default 256MB).
Priority: FSTs (highest hit rate), doc tables (needed for every query),
posting list blocks (largest, lowest priority).

### Per-Query Scan Coalescing And Reuse

Each query owns a short-lived in-memory scratch context. This is not a
shared cache and needs no invalidation beyond dropping it at query end.

- SlateDB segment-discovery work may be coalesced within the query,
  usually per column. When several clauses need nearby or overlapping
  search ranges, the planner may issue one broader scan and reuse the
  returned segment entries across those clauses.
- If two parallel branches still need the same discovery result, use
  single-flight reuse keyed by the discovery request (for example
  `(column, term-kind, search-range)`), so only one branch performs the
  scan and the others await it.
- Immutable segment artifacts already loaded during the query may be
  reused by segment path in addition to any shared LRU cache.
- `check_files_live(candidate_paths)` results may be reused within the
  query when multiple clauses converge on the same candidate-path set.

This avoids repeated planning work inside one request without creating a
cross-query correctness problem.

### Cache Correctness

Cache correctness depends ONLY on the consistent SlateDB checkpoint used
by the query, never on cache freshness:
- Segment files are immutable, addressed by unique paths (UUIDv7).
  Cache key = path. No ETag/version needed.
- Parquet metadata is immutable per file. Same logic.
- The cached handle's current `DbReader` checkpoint determines which
  segments are consulted for that query. LakeSearch re-reads that
  segment-entry metadata from SlateDB each query; caches only affect
  whether immutable bytes come from memory or object storage.
- Invalidation is purely LRU eviction by memory pressure. No TTL
  needed for immutable content keyed by unique paths.

---

## 15. Core Public API

The `lakesearch` library has two layers internally:

- **Core layer** (sync, pure): types, codecs, algorithms, segment
  format. No I/O, no object storage calls. Takes bytes in, gives bytes
  out.
- **Operations layer** (async): index, compact, query, vacuum. Handles
  async I/O, composes core's building blocks via `LakeRuntime`, manages
  SlateDB and catalog interactions.

Both layers live in the same crate. The core layer's types and builders
are public for advanced users; most users interact through the
`LakeSearch` struct.

### Core Builders and Readers

Core never opens a file or makes a network call. Everything works on
`&[u8]` / `Vec<u8>` / `Bytes`:

```rust
// -- Segment writing (indexer and compactor produce these) --

pub struct PostingEntry {
    pub doc_id: DocId,
    pub row_hit_count: u32,
}

pub struct DocTableEntry {
    pub file_ordinal: u32,
    pub row_group: u16,
    pub page_index: u16,
    pub first_row_index: u64,
    pub row_count: u32,
    pub token_count: u32,
}

pub struct SegmentBuilder { .. }
impl SegmentBuilder {
    pub fn new() -> Self;
    pub fn add_file(&mut self, path: &str, row_group_count: u16) -> FileOrdinal;
    pub fn add_page(&mut self, file: FileOrdinal, row_group: u16,
                     page: u16, first_row_index: u64,
                     row_count: u32, token_count: u32) -> DocId;
    pub fn add_posting(&mut self, term: &str, doc_id: DocId, row_hit_count: u32);
    pub fn set_doc_frequency(&mut self, term: &str, df: u32);
    pub fn set_corpus_stats(&mut self, total_rows: u64, total_tokens: u64);
    pub fn build(self) -> Vec<u8>;  // serialized segment file bytes
}

// -- Segment reading (query service consumes these) --

pub struct SegmentReader<'a> { .. }
impl<'a> SegmentReader<'a> {
    pub fn open(data: &'a [u8]) -> Result<Self>;
    pub fn doc_table(&self) -> &DocTable;
    pub fn fst(&self) -> &fst::Map<&[u8]>;
    pub fn reverse_fst(&self) -> &fst::Map<&[u8]>;
    pub fn term_info(&self, term_ordinal: u64) -> Result<TermInfo>;
    pub fn read_posting_list(&self, info: &TermInfo) -> Result<Vec<PostingEntry>>;
    pub fn corpus_stats(&self) -> CorpusStats;
}

// -- Posting list codec --

pub fn encode_posting_list(entries: &[PostingEntry]) -> Vec<u8>;
pub fn decode_posting_list(data: &[u8]) -> Vec<PostingEntry>;

// -- Boolean operations on sorted doc_id arrays --

pub fn intersect(a: &[DocId], b: &[DocId]) -> Vec<DocId>;
pub fn union(a: &[DocId], b: &[DocId]) -> Vec<DocId>;
pub fn difference(a: &[DocId], b: &[DocId]) -> Vec<DocId>;

// -- Tokenizer --

pub fn tokenize(text: &str) -> Vec<String>;

// -- BM25 (stateless math) --

pub fn bm25_score(tf: f32, df: u32, dl: u32, avg_dl: f32, n: u64) -> f32;

// -- Metadata types (SlateDB key-value model) --

pub struct TableConfig { .. }           // indexed_columns
pub struct ColumnSnapshotState { .. }   // last_indexed_snapshot
pub struct CorpusStats { .. }           // total_rows, total_tokens
pub struct SegmentEntry { .. }          // segment_path, min_term, max_term, size_bytes, doc_count, parquet_files, ...
```

These public core APIs intentionally mirror the on-disk segment format:
posting lists expose `row_hit_count`, and doc-table entries expose
`token_count`. Compact and query both need this richer metadata for
exact `doc_frequency` recomputation, corpus-stat recomputation, and
mixed indexed / brute-force BM25.

The core layer has **no traits**. No `StorageBackend`, no `SegmentStore`.
The operations layer calls `object_store` directly and hands bytes to
core. This avoids leaking async boundaries and lifetime complexity into
the pure algorithms.

### Execution Patterns

All operations follow a producer/consumer pipeline that overlaps I/O
and CPU work via bounded channels. I/O runs on tokio, CPU dispatches
to rayon via `LakeRuntime.cpu()`. See `lakesearch-query/src/query/pipeline.rs`
for the reference implementation.

**Query pipeline** (three stages connected by bounded channels):

1. **I/O producers** (tokio tasks, one per file): Stream Parquet pages
   into a work queue. Concurrency limited by semaphore. For indexed
   files, read only candidate pages via RowSelection. For unindexed
   files, stream full row groups sequentially.
2. **CPU dispatcher** (single tokio task): Pulls work items from the
   queue, dispatches to rayon for shadow-column normalization +
   `contains` pre-filter + tokenization + verification + scoring.
   Bounds in-flight CPU work to available threads. Uses `tokio::select!`
   with biased priority to drain completed work before pulling new items.
3. **Coalescer** (single tokio task): Accumulates small output batches
   into target-sized chunks before sending downstream.

Backpressure propagates naturally through bounded channels — a slow
consumer slows the producer. CPU work functions are pure:
`(input, context) → output`, safe to run on rayon with no data races.

**Index pipeline**: Same pattern applied to segment building. I/O
producers stream Parquet pages from object storage. CPU dispatcher
tokenizes rows and accumulates postings. The segment is built
incrementally as pages arrive rather than loading all data first.
After all pages are processed, the segment is finalized, written to
object storage, and committed to SlateDB.

**Compact pipeline**: Fetch input segment files concurrently (I/O),
merge-sort terms with stale filtering on rayon (CPU), write output
segments (I/O), commit to SlateDB. For large merge groups, the merge
itself can be pipelined — read posting blocks from input segments
on-demand rather than loading all segments fully into memory.

### Data Flow Rules

1. **Bytes flow down**: object storage → operations layer → core (for parsing)
2. **Bytes flow up**: core (building) → operations layer → object storage
3. **Core never calls async**: operations layer bridges via `LakeRuntime.cpu()`
4. **Types flow freely**: `SegmentEntry`, `DocId`, `TermInfo` etc. defined in
   core, used by all operations

---

## 16. Crate Structure

```
lakesearch             -- the library. One crate, two layers:
                          Core layer (sync, pure):
                            types, segment format, posting codec, FST,
                            tokenizer, BM25, merge logic.
                          Operations layer (async):
                            LakeSearch struct, index, compact, vacuum,
                            query, Catalog trait + ResolvedTable trait,
                            SlateDB metadata client, query builders.
                          Feature flags:
                            "ducklake" — DuckLake catalog (duckdb dep)
                            "iceberg" — Iceberg catalog (future)
                            "delta" — Delta Lake catalog (future)

lakesearch-server      -- query server binary. Thin wrapper:
                          axum REST + Arrow Flight (tonic).
                          Calls LakeSearch::open() for read-only queries.

lakesearch-cli         -- CLI binary. Thin wrapper:
                          clap argument parsing.
                          Commands: add-index, drop-index, index,
                                   compact, vacuum, query.
                          Query takes `--catalog-uri`, `--table`,
                          and `--json` / `--json-file`.
```

Two binaries, one library. The library is the product.

### Key Dependencies

| Crate | Purpose |
|-------|---------|
| `fst` | Finite state transducer for term dictionary + prefix/suffix search |
| `parquet` (arrow-rs) | Parquet reading with page-level access + RowSelection |
| `arrow` | Arrow array types for column data |
| `arrow-flight` | Arrow Flight gRPC server for streaming query results |
| `object_store` | Abstraction over S3/GCS/Azure/local filesystem |
| `slatedb` | Embedded LSM key-value store on object storage for metadata |
| `duckdb` | DuckLake catalog implementation (behind `ducklake` feature) |
| `axum` | HTTP server for REST search API |
| `tonic` | gRPC framework for Arrow Flight server |
| `serde` / `serde_json` | Metadata serialization |
| `lz4_flex` | Block compression for posting lists |
| `uuid` | Unique file naming |
| `tokio` | Async runtime (I/O, HTTP/gRPC serving) |
| `rayon` | CPU thread pool (FST, posting lists, tokenization, scoring) |
| `moka` | In-memory async LRU cache |

---

## 17. Benchmarking

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
FLIGHT_TABLE = "prod.events"   # server alias `prod` points at the benchmark catalog

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

# -- Benchmark 1: Rare term (high selectivity) --
# Index should win big — prunes almost all files.
def indexed_rare():
    reader = client.do_get(flight.Ticket(
        f'{{"table":"{FLIGHT_TABLE}","search":{{"column":"description","match":"ECONNREFUSED"}}}}'.encode()
    ))
    return conn.sql("SELECT count(*) FROM reader").fetchone()

# IMPORTANT: brute-force baselines use word-boundary regex, not the
# shadow-column substring `contains` pre-filter. Our index does token-based
# matching, so substring '%conn%' would match "disconnect" while the index
# would not. The regex baseline
# with \b is a practical DuckDB-native approximation — not semantically
# identical to our tokenizer (which does Unicode NFC normalization and has
# its own boundary rules), but close enough for performance comparison.

def bruteforce_rare():
    return conn.sql(f"""
        SELECT count(*) FROM read_parquet('{PARQUET_GLOB}')
        WHERE regexp_matches(lower(description), '\\beconnrefused\\b')
    """).fetchone()

bench("rare_term", indexed_rare, bruteforce_rare)

# -- Benchmark 2: Common term (low selectivity) --
# Index may not help much — most pages match anyway.
def indexed_common():
    reader = client.do_get(flight.Ticket(
        f'{{"table":"{FLIGHT_TABLE}","search":{{"column":"description","match":"error"}}}}'.encode()
    ))
    return conn.sql("SELECT count(*) FROM reader").fetchone()

def bruteforce_common():
    return conn.sql(f"""
        SELECT count(*) FROM read_parquet('{PARQUET_GLOB}')
        WHERE regexp_matches(lower(description), '\\berror\\b')
    """).fetchone()

bench("common_term", indexed_common, bruteforce_common)

# -- Benchmark 3: Multi-term AND (high selectivity) --
# Index intersects posting lists — should prune heavily.
def indexed_multi():
    reader = client.do_get(flight.Ticket(
        f'{{"table":"{FLIGHT_TABLE}","search":{{"column":"description","match":"connection timeout upstream","operator":"and"}}}}'.encode()
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

# -- Benchmark 4: Prefix search --
def indexed_prefix():
    reader = client.do_get(flight.Ticket(
        f'{{"table":"{FLIGHT_TABLE}","search":{{"column":"description","match":"conn*"}}}}'.encode()
    ))
    return conn.sql("SELECT count(*) FROM reader").fetchone()

def bruteforce_prefix():
    return conn.sql(f"""
        SELECT count(*) FROM read_parquet('{PARQUET_GLOB}')
        WHERE regexp_matches(lower(description), '\\bconn\\w*')
    """).fetchone()

bench("prefix_search", indexed_prefix, bruteforce_prefix)

# -- Benchmark 5: Text search + aggregation (end-to-end) --
def indexed_agg():
    reader = client.do_get(flight.Ticket(
        f'{{"table":"{FLIGHT_TABLE}","search":{{"column":"description","match":"timeout"}}}}'.encode()
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
| Total rows in snapshot | LakeSearch stats response (`total_rows_in_snapshot`) | Denominator for pruning ratio and coverage reporting |
| Rows scanned | LakeSearch stats response (`rows_scanned`, `indexed_rows_scanned`, `unindexed_rows_scanned`) vs DuckDB `EXPLAIN ANALYZE` | Validates that the index actually pruned data and shows where scan cost came from |
| Segments touched | LakeSearch stats (`segments_touched`) | Shows how much indexed metadata/query work the planner had to consult |
| Unindexed files scanned | LakeSearch stats (`unindexed_files_scanned`) | Makes degraded-path / backfill cost visible |
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
indexed is slower than brute force by more than 10% is a regression signal —
either the index overhead is too high, or the test case should fall back to
brute force automatically (the query planner could use `doc_frequency` to
estimate selectivity and skip the index for very common terms).

The benchmarks above assume a running query server (Flight endpoint).
For local development and CI without a server, the same benchmarks can
run against the library directly using `query()` with an in-memory
object store.

---

## 18. Canonical Test Table

All tests, examples, and worked scenarios use this table schema. Agents
writing tests MUST use this schema so integration tests compose.

**Table name**: `events`

**Parquet schema**:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | `Timestamp(Microsecond, UTC)` | Event time |
| `service` | `Utf8` | Service name (e.g., "api", "worker", "db") |
| `description` | `Utf8` | Free-text log message (primary indexed column) |
| `error_message` | `Utf8` | Error details (secondary indexed column) |
| `status_code` | `Int32` | HTTP status code |
| `response_time_ms` | `Float64` | Request latency |

**Indexed columns**: `description` (always), `error_message` (for
multi-column tests).

**Test data generator** (`write_test_parquet`): cycles through provided
description strings, generates synthetic values for other columns.
Page indices (`offset_index`) are always written.

---

## 19. Worked Example

### Setup

```
Data lake:  DuckLake catalog at ./events.ducklake
            Data at s3://lake/events/
LakeSearch: SlateDB at s3://lake/events/lakesearch/slatedb/
            Segments at s3://lake/events/lakesearch/segments/
```

### Day 1: Initial Index

User inserts 100K rows across 100 Parquet files. DuckLake snapshot = 1.

```
add-index(table="events", column="description")
  -> Creates LakeSearch metadata and records the indexed column.

index(table="events", target_snapshot=Some(1))

1. target = snapshot 1
2. Config: one column "description"
3. description: no snapshot pointer -> last_indexed = None
4. table.files_at_snapshot(1) -> 100 files
5. Build segments for "description" column
   -> 10 segments of ~5MB each
6. Write to s3://lake/events/lakesearch/segments/description/*.seg
7. SlateDB txn:
   - PUT seg|description|{max_term_1}|{seg_1} -> ...
   - ... (10 entries)
   - PUT meta|corpus|description -> { total_rows: 100000, ... }
   - PUT meta|snapshot|description -> { last_indexed: 1 }
   - COMMIT + flush
```

### Day 2: Incremental Writes

User inserts 5K rows in 5 new files. Snapshot = 2.

```
index(table="events")    // no snapshot -> uses current = 2

1. target = 2
2. description: last_indexed = 1
3. table.files_added_between(1, 2) -> 5 new files
4. Build 1 small segment (~500KB)
5. SlateDB txn:
   - PUT seg|description|{max_term}|{seg_11} -> ...
   - UPDATE corpus stats
   - PUT meta|snapshot|description -> { last_indexed: 2 }
   - COMMIT + flush
```

### Day 3: Data Lake Compaction

User runs `ducklake_merge_adjacent_files('lake')`. DuckLake merges the
100 original files into 10 big files. Snapshot = 3. The 5 Day-2 files
are untouched.

```
Step 1: index(table="events")    // indexes new compacted files

1. target = 3
2. description: last_indexed = 2
3. table.files_added_between(2, 3) -> 10 new merged files
4. Build segments for the 10 new files
5. Commit: new segments + description snapshot -> 3

Step 2: compact(table="events")  // clean up stale references

Plan:
1. current_snap = 3
   live_files = table.files_at_snapshot(3) -> 10 merged + 5 day-2 files
2. Scan segments -> 10 original + 1 day-2 + new from step 1
3. 10 original segments reference the 100 pre-compaction files
   -> all stale (fully_stale)
4. Build merge groups for remaining segments by size tier

Execute:
5. Delete 10 fully_stale segments, merge groups as needed
6. Commit
```

### Day 4: Add a New Column (Backfill)

User wants to index the `error_message` column too.

```
add-index(table="events", column="error_message")
  -> Updates config in SlateDB. No snapshot pointer for error_message.

index(table="events")

1. target = 3 (current snapshot, unchanged since day 3)
2. Config: description (active), error_message (active)
3. Snapshot pointers:
   - description: last_indexed = 3 -> up to date, no work
   - error_message: no pointer -> last_indexed = None
4. Catalog calls:
   - description: files_added_between(3, 3) -> empty (or skip entirely)
   - error_message: files_at_snapshot(3) -> 15 files (10 merged + 5 day-2)
5. Build segments for error_message from all 15 files
6. SlateDB txn:
   - PUT seg|error_message|{max_term}|{seg} -> ... (new segments)
   - PUT meta|corpus|error_message -> { total_rows: ..., ... }
   - PUT meta|snapshot|error_message -> { last_indexed: 3 }
   - COMMIT + flush

Both columns now in sync at snapshot 3.
```

### Query During Backfill

If a user queries `error_message` before the backfill index runs:
- error_message has no snapshot pointer -> last_indexed = None
- ALL files are unindexed -> full brute-force scan
- Results are correct, just slow
- After backfill completes, queries use the index

---

## 20. Implementation Phases

Each phase builds on the previous and produces testable, working
functionality. Build on `main` (clean base), cherry-pick the segment
merge primitive from `docs/v2-design`.

### Phase 0: Cherry-Pick and Consolidate

Merge completed work from feature branches and restructure crates.

**Crate consolidation**:
- Merge `lakesearch-core`, `lakesearch-indexer`, and query internals
  into a single `lakesearch` library crate. Keep `lakesearch-server`
  (query server binary) and `lakesearch-cli` (CLI binary) as separate
  thin wrappers.
- Core layer (sync, pure) and operations layer (async) are modules
  within the single crate, not separate crates.

**From `feat/wildcard-query`** (3 commits, clean):
- `QueryTerm { Exact, Prefix, Suffix }` enum and `parse_query()` in
  tokenizer module.
- Updated evaluate, verify, and plan modules for wildcard dispatch.
- Integration tests for prefix and suffix queries.
- Tighten parser validation for the MVP grammar:
  - allow only `prefix*` and `*suffix`
  - reject middle-wildcard, multi-`*`, bare-`*`, and punctuation-splitting
    cases instead of degrading them into surprising exact-term parses
- **This gives us the `QueryTerm` canonical representation** that the
  builder API and QueryPlan depend on.

**From `docs/v2-design`** (cherry-pick only):
- Segment merge primitive (`merge.rs`). Needed for Phase 4.

**Query API builders and LakeSearch struct**:
- Define request boundary types:
  - `QueryBody`: canonical nested query body used by the CLI JSON input
    and embedded callers
  - `SearchRequest`: server/REST/Flight envelope = `{ table, ..QueryBody }`
- Implement centralized table-path parsers:
  - server parser for `catalog_alias.path.to.table`
  - CLI parser for backend-relative `--table`
  - tests for empty input, missing alias, and one-segment backend paths
- Implement `LakeSearch` struct with low-level
  `open(table: Arc<dyn ResolvedTable>)` /
  `open_mut(table: Arc<dyn ResolvedTable>)` and convenience `open_from_uri(...)` /
  `open_mut_from_uri(...)`. The convenience constructors parse the
  catalog URI, resolve the backend-relative table path, and then call
  the lower-level constructors. User never sees internals.
- Implement `QueryRequest::match_text()` and `multi_column()`
  builders that produce the internal `QueryPlan`.
- `QueryPlan` and `ColumnPlan` structs (private to the library).
- Wire builders into REST handler (parse JSON → builder → QueryPlan),
  CLI (parse JSON query body + `--table` → builder → QueryPlan), and
  Flight ticket parsing.
- Score is a boolean (true/false), not a three-mode enum.
- Optional `selectivity_threshold` per query (default 0.0 — always
  use index).

**UX infrastructure** (do it now while touching this code):
- Define `SegmentCache` trait (`get`/`put` on `path → Bytes`).
  Default implementation: `moka::future::Cache` with configurable
  memory budget. Plumb through `LakeSearch` struct so all operations
  use it.
- Structured tracing from the start: `tracing::info!` for operations,
  `tracing::warn!` for degraded paths (brute-force fallback, stale
  filtering), `tracing::error!` for failures. Add as each code path
  is written, not retrofitted.
- Define response stat structs upfront: `QueryStats`
  (`segments_touched`, `candidate_pages`, `total_rows_in_snapshot`,
  `indexed_rows_scanned`, `unindexed_rows_scanned`, `rows_scanned`,
  `rows_matched`, `unindexed_files_scanned`, `elapsed_ms`),
  `CompactResult` (merged, stale
  cleaned, created), `IndexResult` (segments created, files indexed
  per column), `VacuumResult` (files deleted). Wire into responses
  as operations are built.

**Milestone**: Single `lakesearch` crate with strict MVP wildcard
support (`prefix*` / `*suffix` only), query builder API, `LakeSearch`
struct, canonical `QueryPlan`, segment caching, structured tracing,
and response stats. All existing tests pass. Merge primitive available
for Phase 4.

### Phase 1: Core Infrastructure

Foundation libraries inside the `lakesearch` crate.

**SlateDB integration**:
- Internal `MetadataStore` wrapper encapsulating the key layout:
  encode/decode `TableConfig`, `ColumnSnapshotState`, `CorpusStats`,
  `SegmentEntry` to/from `\x00`-separated byte keys and bincode
  values.
- Methods: `get_config`, `get_column_snapshot`, `put_column_snapshot`,
  `get_corpus_stats`, `put_corpus_stats`, `put_segment`,
  `delete_segment`, `scan_segments(column)`,
  `scan_all_segments()`.
- Transaction wrapper: `begin_txn()` → `MetadataTxn` with buffered
  puts/deletes, `commit()`, `flush()`.
- Internal to `LakeSearch` — users never interact with it directly.
- Tests: round-trip each key type, transaction atomicity, scan range.

**Catalog resolution + resolved tables**:
- Internal catalog-construction helpers:
  - build one backend-specific `Catalog` from a CLI `--catalog-uri`
  - build the server's alias -> `Catalog` map from the catalog meta file
- `Catalog` trait: `resolve_table(object_path) -> Arc<dyn ResolvedTable>`.
- `ResolvedTable` trait: `table_location()`, `current_snapshot`,
  `files_at_snapshot`, `files_added_between`,
  `check_files_live`.
- `DuckLakeCatalog` (behind `ducklake` feature flag):
  - Opens `duckdb::Connection`, installs/loads ducklake extension,
    attaches catalog, disables inline data.
  - LakeSearch assumes it is connecting to an existing DuckLake.
    `DATA_PATH` is not part of the read/query contract here; it is
    only required when creating a new DuckLake outside LakeSearch.
  - `resolve_table` interprets the backend-relative path, resolves
    DuckLake metadata for that table, and returns
    `Arc<DuckLakeResolvedTable>`.
  - `DuckLakeResolvedTable` implements `ResolvedTable`; its methods map
    to SQL queries against
    `__ducklake_metadata_{catalog}.ducklake_data_file`.
  - Clear errors for invalid snapshots, missing tables.
- `MockCatalog` for unit tests (no DuckDB dependency).
- Tests:
  - table-ref parsing and error cases
  - mock resolved-table implementation exercises the resolved-table interface
  - catalog meta file loads alias config correctly
  - DuckLake integration test (`#[ignore]`)

**LakeSearch operations — add_index, drop_index**:
- `ls.add_index(column, tokenizer)` (requires `open_mut`):
  - If metadata does not exist yet: create SlateDB at
    `{table.table_location()}/lakesearch/slatedb/` and write
    `meta|config`.
  - Reads config. If the column is already active, return an error.
  - If the column is dropped, reactivate that same column entry and
    update its tokenizer. Otherwise append a new column (status=active).
  - No snapshot pointer created (backfill happens on next `index`).
- `ls.drop_index(column)` (requires `open_mut`):
  - Single transaction: update config (status=dropped), delete all
    segment keys, corpus stats, and snapshot pointer for that column.
- CLI wrappers: `lakesearch add-index` and `lakesearch drop-index`
  call through `LakeSearch` with `--catalog-uri` + backend-relative
  `--table`.
- Tests: add-index on a new table → verify config created, add-index
  on an existing table with a new column → verify column appended,
  add-index on an already-active column → verify error, drop-index →
  verify segments/stats/pointer deleted, drop-index then add-index on
  the same column with a new tokenizer → verify reactivation +
  backfill-needed state, uninitialized `drop-index` returns the "run
  add-index first" error.

**Milestone**: `add_index()` initializes metadata, `drop_index()`
removes a column cleanly, and both work against DuckLake + SlateDB.
All metadata operations tested with mock catalog.

### Phase 2: Query Path Improvements

Fix the query layer before building new features. Items within this
phase are independent — can be done in any order or in parallel.

**Canonical shadow-column pre-filter on verification path**:
- Add a pre-filter to `verify_batch` (indexed candidates): build
  temporary lowercase + NFC Arrow arrays or a shadow `RecordBatch` for
  the searched columns, run vectorized substring `contains` per query
  term, AND/OR the boolean masks, then filter the original batch before
  tokenization.
- Expected: tokenization on ~10 rows instead of 8192 per candidate
  page. Orders-of-magnitude CPU reduction.
- Tests: same results with and without the pre-filter. Benchmark
  shadow-column filtering versus direct tokenize-every-row and keep the
  optimization only where it wins.

**QueryStats population + accounting**:
- Populate `total_rows_in_snapshot` from the queried lake snapshot and
  track `indexed_rows_scanned` / `unindexed_rows_scanned` as actual
  row-level verification work on the indexed and brute-force paths.
- Set `rows_scanned = indexed_rows_scanned + unindexed_rows_scanned`.
- Tests: fully indexed query, mixed indexed/unindexed query, and
  brute-force-only query all report the expected split counters and
  preserve the sum invariant.

**Global df(t) aggregation for BM25**:
- After loading candidate segments, sum `doc_frequency` across all
  segments per query term. Read global `total_rows` and `total_tokens`
  from SlateDB corpus stats.
- For ranked queries with an unindexed tail, brute-force pass 1 also
  accumulates per-column `delta_rows`, `delta_tokens`, and `delta_df`
  so final BM25 uses the full queried corpus, not just the indexed
  subset.
- Thread `global_df` and `global_corpus_stats` through
  `SharedQueryContext` to scoring.
- Tests: two segments with overlapping terms → verify scores use
  summed df. Mixed indexed/unindexed query → verify scores match the
  same corpus after full indexing. Golden test with known inputs.

**Selectivity threshold / brute-force cutoff**:
- Implement `selectivity_threshold` estimation from per-term
  `doc_frequency` and corpus stats before posting-list decoding.
- If estimated selectivity exceeds the threshold, skip indexed
  candidate generation for that scope and use brute force instead.
- Tests: rare term stays indexed, common term flips to brute force at
  the configured threshold, threshold `0.0` preserves the always-index
  behavior.

**Staged row-range reduction for cross-segment / cross-column queries**:
- Reduce each `ColumnPlan` leaf to row ranges before any Parquet read.
- Same-segment terms use the fast `doc_id` intersection/union path.
- Split-sibling or cross-segment terms resolve to page locations first,
  then to row ranges.
- Suffix queries do not get segment-file pruning in the MVP: they load
  all segments for the column, then use the reverse FST inside each
  segment and still benefit from normal page/Parquet pruning.
- Cross-column `AND` / `OR` combines row ranges according to the
  enclosing `QueryExpr` tree, then the reader performs one Parquet pass
  for the combined `RowSelection`.
- Tests: same-segment AND, split-sibling AND, suffix query, cross-column
  AND, and cross-column OR all return the same rows as brute force.

**Stale filtering on the query path**:
- Collect Parquet file paths from candidate segments and call
  `ResolvedTable::check_files_live(...)` once per query scope.
- Filter stale page hits before row-range reduction / Parquet reads.
- Tests: query with stale segments before compact returns the same rows
  as after compact; stale candidates never leak into results.

**Two-pass brute-force projection**:
- Pass 1: stream only the searched columns. For filter queries, build
  lowercase + NFC shadow Arrow arrays, apply vectorized substring
  `contains`, then tokenize survivors and collect matching row indices.
- Ranked mixed-coverage queries (`score: true` with unindexed files)
  use a heavier pass 1: tokenize every row once so BM25 can accumulate
  exact `delta_rows`, `delta_tokens`, and `delta_df`.
- Pass 2: build RowSelection from matches, read other projected
  columns for matches only.
- I/O reduction: ~1% of reads for non-searched columns vs 100%.
- Tests: projected columns correct for matches, absent for non-matches.

**Query server on SlateDB**:
- The server loads its catalog meta file once, parses incoming
  `table` strings into `catalog_alias + backend path`, resolves tables
  lazily through the configured `Catalog` implementations, and caches
  read-only `LakeSearch::open(table)` handles for hot tables.
- REST / Flight stay strictly on the read path:
  - accept `SearchRequest`
  - resolve the table lazily
  - return "run add-index first" if metadata is missing
  - do not expose mutating operations on the server
- Query planning reads segment entries from SlateDB via
  `scan_segments(column)` instead of walking manifest lists.
- Stale filtering: `ResolvedTable::check_files_live(candidate_paths)`.
- Unindexed detection: `ResolvedTable::files_added_between(last_indexed,
  current)`.
- Remove: `read_current`, `read_metadata`, manifest list loading.

**Segment caching update**:
- `ObjectCache` caches segment file bytes by path (immutable).
- Query planning reads active segment entries fresh from SlateDB each
  time; do not cache mutable SlateDB scan results across queries.
- Within one query, coalesce same-column SlateDB scans and single-flight
  reuse repeated discovery / immutable-artifact work in a query-local
  scratch context.
- Remove manifest/manifest-list caching.
- Cached read-only handles reuse a `DbReader` that tracks the latest
  persistent manifest by polling at `manifest_poll_interval`; each query
  uses that reader's current checkpoint consistently.
- Parquet metadata caching unchanged.
- Instrumentation / tests: verify same-column clauses reuse one
  coalesced discovery path within a query, and parallel branches
  single-flight repeated segment opens instead of duplicating them.

**Milestone**: Query path reads from SlateDB + catalog. Indexed
candidate generation reduces to row ranges before any Parquet read.
BM25 uses full-corpus stats (including ranked mixed-coverage tails).
Canonical shadow-column pre-filter on indexed path. Two-pass brute-force. Server
resolves tables lazily from the catalog meta file and stays strictly
read-only. All existing query tests pass with new backend.

### Phase 3: Indexer Update

Port the indexer to v3 architecture and pipeline it.

**Port to SlateDB + catalog**:
- `ls.index(IndexRequest)` internally:
  - Resolves target snapshot from the resolved table handle
    (or uses provided).
  - Reads config and per-column snapshot pointers from SlateDB.
  - Groups columns by `last_indexed`, one catalog call per distinct
    value.
  - Builds segments per column (core logic unchanged).
  - Writes segments to `{table.table_location()}/lakesearch/segments/`.
  - Single SlateDB transaction: put segment entries, update corpus
    stats, advance all column snapshot pointers. Flush.
- Remove: CAS commit, JSON metadata/manifest writing, batch_id dedup.

**Pipeline the indexer**:
- Change from load-all-then-process to producer/consumer:
  - I/O producers (tokio tasks): stream Parquet pages, one per file.
    Bounded by semaphore.
  - CPU consumer (rayon): tokenize rows, accumulate postings in
    `SegmentBuilder`. Sequential processing on one rayon thread
    (accumulation order doesn't matter for correctness).
  - After all pages: finalize segment (FST, posting lists, doc table).
- Overlaps I/O and CPU.

**Per-page row-hit accounting**:
- While tokenizing rows, track term presence per row and aggregate it
  into per-page `row_hit_count` values stored alongside posting
  `doc_id`s.
- Preserve row-level `doc_frequency` in the term info table and
  `token_count` in the doc table.
- Tests: page with repeated term occurrences in one row vs many rows →
  verify `row_hit_count`, row-level `doc_frequency`, and corpus stats
  all match expectations.

**Populate doc-table row ranges from Parquet `offset_index`**:
- Validate that every indexed Parquet file has `offset_index`
  available for the indexed column.
- Populate `first_row_index` and `row_count` in the doc table directly
  from `offset_index` at index time so query-time cross-column
  intersection does not need extra Parquet metadata reads.
- Reject files missing `offset_index` with a clear ingest-time error.
- Tests: doc table row ranges match Parquet metadata exactly; missing
  `offset_index` is rejected deterministically.

**Monotonic target-snapshot semantics**:
- `index(target_snapshot)` may backfill lagging or uninitialized
  columns up to `target_snapshot`.
- Columns already ahead of `target_snapshot` remain unchanged.
- If every active column is already ahead of `target_snapshot`, fail
  instead of rewinding any snapshot pointer.
- Tests: mixed lagging/ahead columns backfill correctly; ahead columns
  never move backward; pure rewind fails.

**Missing column handling**:
- Skip files missing the indexed column (log debug). Snapshot pointer
  still advances. No terms contributed.

**Backfill verification**:
- Integration test: create table with column A, index, add-index
  for column B, run index. Column A does no work, column B indexes
  all files. Both pointers at target.
- Test incremental backfill: index to snapshot 5, then 10.

**Milestone**: `add_index` → `index` → `query` works end-to-end with
DuckLake + SlateDB. Indexer pipelined. Backfill tested. Uninitialized
tables fail cleanly until `add-index` is run.

### Phase 4: Compaction + Term-Range Splitting

This is the most complex phase.

**Port and extend merge primitive**:
- Merge primitive from `docs/v2-design`: merge-sort by term, union
  posting lists, concatenate doc tables, renumber doc_ids, rebuild FST.
- Posting entries carry `row_hit_count` (`# rows in that page
  containing the term`), so compact can recompute row-level
  `doc_frequency` exactly after stale filtering.
- Add `stale_paths: &HashSet<String>` parameter. Skip stale doc table
  entries and their posting list doc_ids. Recompute `doc_frequency`
  as `sum(row_hit_count)` over survivors, and recompute corpus stats
  from surviving doc-table entries. Omit dead terms from output FST.
- Tests: merge two segments, merge with stale paths, merge where all
  entries stale, single-segment rewrite, partial-stale segment where
  surviving pages have mixed `row_hit_count` values.

**Unified plan-then-execute compact algorithm**:
- `ls.compact(CompactRequest)` internally:
  - Plan: `files_at_snapshot(current)` → `live_paths`. Scan all
    segments. Annotate staleness. Group by column, then size tier.
    Build merge groups. Log plan.
  - Execute: per merge group, read segments, call `merge_segments`,
    write output. Groups are independent — parallelizable.
  - Commit: single SlateDB transaction — delete stale/consumed
    segments, put new segments, update corpus stats per column.
    Flush.
- Compact always queries the **current** lake snapshot for liveness and
  never advances `last_indexed_snapshot`; only `index()` moves those
  per-column pointers.
- Tests: no-op compact, fully stale deletion, partial stale rewrite,
  size-tiered merge, full lifecycle with data lake compaction,
  compact-on-stale-data without a preceding index, and snapshot
  pointers unchanged after compact.

**Term-range splitting**:
- When merge output exceeds `target_segment_size`, split by term
  range. Track output size during sorted iteration, finalize and
  start new segment at threshold.
- Each sibling is a fully independent segment with its own doc table
  and dense doc_ids. No shared doc table. Cross-sibling AND uses
  page-location intersection (§ 7).
- Store `min_term`/`max_term` per sibling in SlateDB segment entry.
- Tests: merge within target → one output. Merge exceeding target →
  split outputs with disjoint term ranges.

**Page-location intersection for cross-segment AND**:
- When AND query terms span different segments, resolve doc_ids to
  `(file, row_group, page)` tuples per segment's doc table, then
  intersect tuple sets. Fast u32 path when terms are in same segment.
- Tests: split siblings, AND across both → correct. Unsplit → same.

**CLI**: `lakesearch compact --catalog-uri {uri} --table {name}`.

**Milestone**: Full index → compact → query lifecycle. Stale cleanup.
Term-range splitting. Cross-segment AND. Exact `doc_frequency`
recomputation after stale filtering.

### Phase 5: Vacuum

**Vacuum implementation**:
- `ls.vacuum(VacuumRequest)` — read-only, uses `open` (not
  `open_mut`). LIST segments in object storage, diff against SlateDB,
  delete orphans older than grace period.
- Vacuum only manages LakeSearch segment files. SlateDB's own GC
  remains internal to SlateDB and is not touched here.
- UUIDv7 segment IDs prevent collision with successor writers.
- CLI: `lakesearch vacuum --catalog-uri {uri} --table {name} [--grace-period 1h]`.
- Tests: orphan files deleted, live files preserved, grace period
  respected, SlateDB-managed files never targeted.

**Milestone**: Full lifecycle works — add-index, index, compact,
vacuum, query. All operations tested end-to-end.

### Phase 6: Benchmarks

**Criterion benchmark suite**:
- `benches/posting.rs`: encode/decode dense/sparse 10K.
- `benches/boolean.rs`: intersect/union/difference at various sizes.
- `benches/tokenizer.rs`: throughput MB/sec.
- `benches/segment.rs`: build time, cold read.
- `benches/e2e.rs`: rare term, common term, multi-term AND, prefix,
  suffix, split-sibling cross-segment AND, ranked mixed-coverage
  query, and index throughput.
- All use in-memory object store.

**Benchmark harness** (Python):
- Indexed vs brute-force via Arrow Flight.
- Test matrix: data sizes, selectivities, query types, fully indexed vs
  partially indexed tails, and `selectivity_threshold` cutovers.
- Regression gate: >10% investigated.

**Milestone**: Performance validated. Benchmark suite in CI.

### Phase 7: Additional Catalog Implementations

Tracing, caching, and stats are built in Phase 0/1. This phase adds
support for production data lake formats beyond DuckLake.

**Iceberg catalog** (behind `iceberg` feature):
- `IcebergCatalog` using `iceberg-rust` crate. Reads table metadata
  from REST catalog or object storage.
- Backend-owned path parsing:
  - last segment = table name
  - preceding segments = namespace path
- Manifest caching: LRU by path (immutable files), cache current
  manifest list (re-read on snapshot advance), incremental updates
  (diff old/new manifest list, read only new manifests).
- Steady-state `check_files_live` and `files_added_between` are
  in-memory scans over cached manifests. No S3 reads.
- Integration tests with `#[ignore]`.

**Delta Lake catalog** (behind `delta` feature):
- `DeltaCatalog` using `deltalake` crate. Reads `_delta_log/` commit
  entries. Checkpoint-based state reconstruction.
- Backend-owned path parsing follows Delta's table naming rules for the
  configured catalog / root.
- Integration tests with `#[ignore]`.

**DataFusion integration**:
- Implement a DataFusion `TableProvider` that wraps `LakeSearch::query`
  as a scan node. A SQL query like
  `SELECT * FROM lakesearch_query('prod.events', '{"search":{"column":"description","match":"error timeout"}}')`
  pushes the canonical query shape into LakeSearch and returns an Arrow
  `RecordBatchStream` that DataFusion consumes natively.
- Integrate into DataFusion query plans: LakeSearch handles text
  search pruning, DataFusion handles the rest (joins, aggregations,
  window functions, ORDER BY).
- SQL parsing: register a custom function or table-valued function
  so users can compose text search with standard SQL without leaving
  the DataFusion execution context.
- This enables the same "text search + OLAP" story as the Arrow
  Flight path but without a separate server — LakeSearch runs
  in-process as a DataFusion extension.

**Python bindings** (if needed):
- PyO3 bindings for `LakeSearch::open()`, `index()`, `compact()`,
  `vacuum()`, `query()`.
- Deferred unless user demand is clear.

**Milestone**: Iceberg catalog for production server deployments.
Delta catalog as alternative. DataFusion integration for in-process
text search + SQL. DuckLake remains the local dev default.

### Phase 8: Deferred Query Language Extensions

These are intentionally **not** part of the MVP. They are listed here so
the roadmap is exhaustive, but the earlier phases are fully
implementable without them.

**Negation / richer wildcard support**:
- Add `not` back into the public query grammar and private `QueryExpr`
  only after the planner/runtime contract is fully designed.
- Add richer wildcard forms only after the MVP prefix/suffix model is
  stable:
  - middle wildcard / contains matching
  - multi-`*` globs
  - literal escaping
  - regex queries
- Define explicit planner behavior for these features rather than
  silently degrading to surprising parses.
- Tests: correctness against brute force, clear validation errors for
  malformed syntax, and benchmark gates for pathological expansions.

**Milestone**: Extended query language beyond the MVP, implemented only
after the prefix/suffix-only grammar and positive boolean model have
been proven in production.

---

## 21. Design Decisions Summary

All design questions raised during the review process have been
resolved and inlined into their respective sections:

- **Table resolution model**: server/REST/Flight use
  `catalog_alias + backend path`; the backend owns the remaining path
  segments (§ 2, architecture; § 10, catalog resolution)
- **Catalog abstraction**: `Catalog::resolve_table(...)` returns a
  table-bound `ResolvedTable` trait object; there is no separate
  backend enum wrapper or registry trait
  (§ 10, catalog resolution and table metadata)
- **Resolved-table seam**: `open` / `open_mut` take
  `Arc<dyn ResolvedTable>` directly; backend-specific catalogs stay
  behind that trait boundary (§ 2, architecture; § 10, catalog resolution)
- **CLI contract**: all operations are stateless via `--catalog-uri`
  + backend-relative `--table`; `query` accepts only `--json` /
  `--json-file` for the nested query body (§ 2, CLI; § 8, query model)
- **Request model**: `QueryBody` is the canonical nested query body;
  REST/Flight wrap it in `SearchRequest { table, .. }`, while the CLI
  supplies table identity out of band (§ 8, query model; § 9, Flight)
- **Initialization model**: there is no public `init`; `add-index`
  creates metadata and all other verbs fail with "run add-index first"
  until initialization exists (§ 2, CLI; § 12, API specs; § 13, column lifecycle)
- **One active index per column in MVP**: `add-index` on an already
  active column is an error; changing tokenizer is `drop-index` +
  `add-index` + `index`; multiple indexes per column are future work
  (§ 10, key layout; § 12, API specs; § 13, column lifecycle)
- **Cross-column query surface**: no public `multi_match`; one-column
  `match` + explicit boolean composition is the only external query
  model (§ 8, query model)
- **Canonical query grammar**: public JSON uses `match`, recursive
  `and` / `or`; the private plan is the same recursive positive-only
  `QueryExpr` tree. Negation (`not`) is future work, not MVP
  (§ 8, query model)
- **Default tokenizer name**: `whitespace_lowercase`
  (§ 4, tokenization; § 13, column lifecycle)
- **Snapshot identifier type**: `SnapshotId(pub i64)`; all supported
  backends must expose a non-negative, totally ordered 64-bit integer
  snapshot / version identifier (§ 10, catalog resolution)
- **Key separator**: `\x00` (§ 11, key layout)
- **SlateDB keying**: keys are table-local because each resolved table
  has its own SlateDB instance; there is no public `table_id`
  (§ 2, storage layout; § 10, resolved tables; § 11, key layout)
- **Index target semantics**: per-column snapshot pointers are
  monotonic; explicit older targets backfill only lagging columns and
  never rewind ahead columns; a pure rewind fails (§ 12a, index API)
- **Stale filtering in merge**: `stale_paths` parameter on the merge
  primitive (§ 12b, compact API)
- **Corpus stats accuracy**: `token_count` lives in the doc table and
  `row_hit_count` lives in posting entries, enabling exact
  `total_tokens` and row-level `doc_frequency` recomputation during
  stale filtering (§ 3, segment file format; § 12b, compact API)
- **Mixed indexed + brute-force BM25**: ranked queries score over the
  full queried corpus by adding `delta_rows`, `delta_tokens`, and
  `delta_df` from the unindexed tail before final scoring
  (§ 5, BM25 scoring; § 8, query model; § 12c, query path)
- **Segment ID generation**: UUIDv7, time-sortable (§ 2, storage layout)
- **SlateDB per-table**: One instance per table (§ 2, storage layout)
- **Compact does not advance snapshot pointer**: Only index advances it
  (§ 12b, compact API)
- **Query stats schema**: `segments_touched`, `candidate_pages`,
  `total_rows_in_snapshot`, `indexed_rows_scanned`,
  `unindexed_rows_scanned`, `rows_scanned`, `rows_matched`,
  `unindexed_files_scanned`, `elapsed_ms` (§ 8, query model)
- **Cheap row filter semantics**: build temporary lowercase + NFC
  shadow Arrow arrays / batches and run vectorized substring
  `contains` there before tokenization; do not use raw-string `ILIKE`
  because it is not aligned with tokenizer normalization
  (§ 4, tokenization; § 8, query model; Phase 2 roadmap)
- **Stale index queries**: Always return correct results, report
  `unindexed_files_scanned` in QueryStats (§ 12c, query path)
- **Query-time stale filtering**: always use targeted
  `check_files_live(candidate_paths)` over the candidate segments'
  Parquet paths; do not skip liveness checks solely because the column
  snapshot pointer equals the current lake snapshot (§ 12c, query path)
- **Cross-segment / cross-column execution**: reduce each clause to row
  ranges first, combine row ranges according to the `QueryExpr` tree,
  then fetch Parquet once for the combined `RowSelection`
  (§ 8, query model; § 12c, query path)
- **Canonical row identity**: `(file_path, row_group,
  row_index_within_row_group)` is the stable row key for dedupe,
  merge, and tie-breaking; the row index is 0-based within the Parquet
  row group and comes from `PageLocation.first_row_index + offset`
  (§ 8, query model)
- **Prefix segment pruning**: prefix queries route by interval overlap
  with `[prefix, prefix_upper_bound(prefix))`, not by treating the raw
  prefix string as an exact term (§ 8, query model; § 11, key layout;
  § 12c, query path)
- **Query-time catalog latency**: Iceberg implementations should cache
  manifests; keeping index up to date reduces unindexed-tail catalog
  work, but queries still perform targeted
  `check_files_live(candidate_paths)` lookups (§ 10, Iceberg manifest
  caching; § 12c, query path)
- **`files_at_snapshot` memory at scale**: Query uses
  `check_files_live` with small input set; compact accepts full
  materialization as a batch cost. Users should run data lake
  compaction regularly for high-write tables (§ 10, catalog resolution;
  § 12c, query path)
