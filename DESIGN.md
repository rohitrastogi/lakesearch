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

### Library-First Deployment Model

The `LakeSearch` struct is the primary API surface. It wraps all
internal infrastructure (SlateDB, catalog, object store, runtime)
behind a single handle. The user points it at their table and calls
methods — no direct interaction with SlateDB, catalogs, or object
store internals.

```rust
// Read-only handle (query, vacuum) — safe for concurrent use, no fencing
let ls = LakeSearch::open("ducklake:./events.ducklake", "events").await?;
ls.query(query_request).await?;
ls.vacuum(VacuumRequest { grace_period: None }).await?;

// Read-write handle (all operations) — takes writer lock, fences other writers
let ls = LakeSearch::open_mut("ducklake:./events.ducklake", "events").await?;
ls.add_index("description", "default").await?;
ls.drop_index("description").await?;
ls.index(IndexRequest { target_snapshot: None }).await?;
ls.compact(CompactRequest {}).await?;
ls.query(query_request).await?;
ls.vacuum(VacuumRequest { grace_period: None }).await?;
```

`open` parses the catalog URI, constructs the appropriate
`DataLakeCatalog` implementation, resolves the table's storage path,
and opens SlateDB in read-only mode (`DbReader`). `open_mut` does the
same but opens SlateDB in writer mode (`Db`), which fences out any
other writer on the same table. Calling `index()` or `compact()` on
a read-only handle returns a clear error.

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

The query server uses `open` (read-only, multiple instances). The CLI
uses `open_mut` for index/compact and `open` for query/vacuum.

### CLI

Thin wrapper around the library:

```
lakesearch init         --table events --column description --catalog ducklake:./events.ducklake
lakesearch add-index   --table events --column description [--tokenizer default]
lakesearch drop-index  --table events --column description
lakesearch index        --table events [--snapshot 5]
lakesearch compact      --table events
lakesearch vacuum       --table events [--grace-period 1h]
lakesearch query        --table events --column description --query "error timeout" [--limit 100]
```

`init` connects to the catalog, verifies the table exists, resolves
the table's storage path (e.g., DuckLake's `DATA_PATH`, Iceberg's
`table.location`), and creates the SlateDB instance at
`{table_path}/lakesearch/slatedb/`. The `table_id` is the user-provided
`--table` name (e.g., "events") — it's a human-readable identifier used
as a key prefix in SlateDB, not a UUID. Writes `meta|{table_id}|config`
with the indexed columns, tokenizer settings, and catalog connection
info. No snapshot pointers or corpus stats yet — those are created by
the first `index` call. Idempotent — re-running with the same config
is a no-op.

### Server (Optional)

The query service is the only server component. Its config is a list
of table paths — everything else (columns, catalog URI, segment
metadata) is already in each table's SlateDB:

```yaml
tables:
  - s3://bucket/warehouse/events/
  - s3://bucket/warehouse/logs/
```

On startup, the server opens `LakeSearch::open()` (read-only) for each
table path and serves queries against them. No column config, no
catalog URIs in the server config — those are in SlateDB.

A long-running server caches:
- SlateDB reads (segment index entries) — avoids repeated object store reads.
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
+--------------------------------------+
| Block Header (15 bytes)              |
|   num_docs: u16                      |
|   min_doc_id: u32     <- skip-ahead  |
|   bit_width: u8       <- bits/delta  |
|   flags: u8           <- bit 0: LZ4  |
|   compressed_size: u32               |
|   uncompressed_size: u16             |
+--------------------------------------+
| Compressed Data                      |
|   delta-encoded doc_ids, bit-packed  |
|   (optionally LZ4-compressed)        |
+--------------------------------------+
```

Encoding within a block:
1. Delta-encode the sorted `doc_id` array (dense IDs -> small deltas, often 1)
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

## 4. Tokenization

MVP tokenizer: `whitespace_lowercase`

1. Split on Unicode whitespace and punctuation (`char::is_alphanumeric` boundaries)
2. Lowercase (Unicode-aware)
3. Normalize to NFC
4. Filter tokens shorter than 1 character or longer than 256 bytes
5. Each surviving token becomes a term in the posting list
6. Count occurrences per row for BM25 `doc_frequency` aggregation at index time

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

1. **From the segment file**: read `doc_frequency(t)` (per term, from term info
   table) and corpus stats (`total_rows`, `total_tokens` -> `avg_dl`)
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
- `N` = `total_rows` from global corpus stats (SlateDB)
- `df(t)` = sum of `doc_frequency` across all segments containing the term (see Cross-Segment Scoring)
- `k1 = 1.2`, `b = 0.75` (standard defaults)

### Cross-Segment Scoring

When a query spans multiple segments, BM25 uses **global statistics**
aggregated across all segments. There is no segment-local scoring
approximation — the correct version costs the same as the approximate
one.

**Why global stats are free**: By the time we score, we've already
loaded all candidate segments (for posting list evaluation). Each
segment's term info table — loaded in the speculative tail read —
contains `doc_frequency` for each term. Global corpus stats (`N`,
`total_tokens`) are already read from SlateDB at query start.

**Aggregation**:
```
global_N = corpus_stats.total_rows           (from SlateDB)
global_avg_dl = if global_N > 0 { corpus_stats.total_tokens / global_N } else { 0.0 }
For each query term t:
  global_df[t] = sum(segment.term_info(t).doc_frequency
                     for segment in loaded_segments
                     if segment.fst.contains(t))
```

This is just an addition across values already in memory. No extra I/O.

**Zero edge case**: If `global_N == 0` (all segments for a column were
stale-deleted by compact, or column was just added and never indexed),
`avg_dl` is 0 and BM25 scores are 0. The query returns no indexed
results — only brute-force results if unindexed files exist. No
divide-by-zero.

**Correctness**: Summing df(t) across segments is safe because no
Parquet file appears in two live segments simultaneously. Each index
call creates segments for a specific batch of new files. Compaction
atomically replaces old segments with new merged ones in a single
SlateDB transaction. Term-range split segments cover the same files
but partition the term space — a given term appears in exactly one
sibling, so df(t) is never counted twice.

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
  multi_match() ─────┼──→  QueryPlan (canonical) ──→  plan / evaluate / verify
  multi_column() ────┘
  REST JSON ─────────┘ (parsed by handler)
  CLI flags ─────────┘ (parsed by clap)
```

#### Library Builders (Rust)

Three entry points cover all query patterns:

```rust
// 90% case: search one column for text (wildcards supported: conn*, *tion)
let req = QueryRequest::match_text("description", "error timeout")
    .operator(BoolOp::And)  // default Or
    .select(&["timestamp", "service"])
    .score(true)
    .limit(10);

// Search box: same text across multiple columns
let req = QueryRequest::multi_match("connection refused", &["description", "error_message"])
    .limit(50);

// Cross-column predicates: different text per column
let req = QueryRequest::multi_column(BoolOp::And)
    .column("description", "error")
    .column("status_code", "500")
    .limit(10);
```

Builders construct the internal `QueryPlan`. The user never imports
enum variants or constructs AST nodes. Wildcards (`*`) are embedded in
the query text — `parse_query()` tokenizes the text and produces typed
`QueryTerm { Exact, Prefix, Suffix }` values that the evaluator
switches on.

#### CLI

Flags map to builder calls:

```bash
# match_text
lakesearch query --table events --column description --match "error timeout" --operator and --limit 10

# multi_match
lakesearch query --table events --columns description,error_message --match "connection refused" --limit 50

# multi_column
lakesearch query --table events \
  --search "description:error" \
  --search "status_code:500" \
  --combine and --limit 10
```

#### REST / Flight

JSON maps to the same builders. The REST shapes are intentionally
close to Elasticsearch's `match` / `multi_match` / `bool` patterns.

#### Internal Canonical Representation (QueryPlan)

Private to the library. The evaluator switches on this — never on raw
text or user-facing types.

```rust
// Private — produced by builders, consumed by planner/evaluator.

enum BoolOp { And, Or }   // used for both within-column and cross-column

struct QueryPlan {
    searches: Vec<ColumnPlan>,
    combine: BoolOp,                 // cross-column And | Or (irrelevant for single column)
    select: Vec<String>,
    limit: Option<usize>,
    score: bool,
    selectivity_threshold: f64,      // default 0.0 (always use index)
}

struct ColumnPlan {
    column: String,
    terms: Vec<QueryTerm>,           // from parse_query()
    operator: BoolOp,                // within-column And | Or
}

// From tokenizer module (currently lakesearch-core/src/tokenizer.rs;
// moves during crate consolidation in Phase 0)
enum QueryTerm {
    Exact(String),              // FST exact lookup
    Prefix(String),             // FST prefix iteration → union posting lists
    Suffix(String),             // reverse FST prefix iteration → union
}
```

NOT is not expressible through the builder API. For queries requiring
NOT clauses, use the cross-column JSON syntax in REST/Flight. NOT
support in builders may be added later if demand warrants it.

`QueryTerm` is the atomic unit the evaluator dispatches on:
- `Exact` → single FST lookup → one posting list
- `Prefix` → forward FST prefix iterator → union of posting lists
  (bounded by `MAX_WILDCARD_EXPANSION`)
- `Suffix` → reverse FST prefix iterator → union of posting lists

If a wildcard expands to more than `MAX_WILDCARD_EXPANSION` (1024)
terms, return an error: "prefix '{prefix}*' expands to N terms,
exceeding limit of 1024." No silent truncation, no brute-force
fallback. The user narrows their query.

`ColumnPlan.operator` determines how term-level posting lists are
combined (AND intersection / OR union). `QueryPlan.combine` determines
how column-level results are joined (AND: row must match all columns,
OR: row matches any column). Both AND and OR sum per-column BM25
scores. OR rows matching more columns rank higher. Ties broken by
`(file_path, row_index)` for deterministic ordering.

### Query Language

Queries can search across **multiple indexed columns** with boolean operators.
Searches on different columns can be combined with AND/OR at the top level.

The query language provides wildcard support (`conn*` for prefix,
`*tion` for suffix) and a `match` shorthand that mirrors Elasticsearch's
most common query pattern. Wildcards are parsed by `parse_query()` into
typed `QueryTerm` values.

#### Match (High-Level Shorthand)

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

When `operator` is `"or"` (or omitted), any token matching is sufficient.
When `operator` is `"and"`, all tokens must match. Wildcards are
supported in the text: `conn*` for prefix, `*tion` for suffix. This
covers the vast majority of real-world text search queries with minimal
ceremony.

#### Cross-Column Search

For different queries per column, use the `and` / `or` wrapper:

```json
{
  "table": "events",
  "search": {
    "and": [
      { "column": "description", "match": "error" },
      { "column": "status_code", "match": "500" }
    ]
  },
  "limit": 10
}
```

#### Multi-Column Match Shorthand

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

### Term Resolution

Within a single column, `parse_query()` produces `QueryTerm` values
that the evaluator dispatches on:

- **Exact** (`timeout`): FST exact lookup → one posting list
- **Prefix** (`conn*`): forward FST prefix iteration → union of
  posting lists (bounded by `MAX_WILDCARD_EXPANSION = 1024`)
- **Suffix** (`*tion`): reverse FST prefix iteration → union of
  posting lists (bounded)

The `operator` (AND/OR) determines how term-level posting lists are
combined: AND uses sorted-array intersection, OR uses sorted-array
union on `u32` doc_id values.

**Important:** all page-level boolean operations are **approximate**. They
identify candidate pages that *may* contain matching rows, but a page-level
AND does not guarantee every row in the page satisfies the full predicate. The
row-level verification step (see execution pipeline) uses an ILIKE pre-filter
(SIMD substring check) to cheaply discard most non-matching rows, then
tokenizes only survivors to evaluate the complete boolean query and eliminate
false positives.

#### NOT Semantics

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

1. For each column's search, evaluate boolean query -> set of `doc_id`s
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

1. **Plan**: Parse query, load segment entries from SlateDB and catalog state,
   identify relevant segments per column.
   Expand `match` and `multi_match` nodes into boolean trees of `term` nodes.
2. **Selectivity Estimation**: For each term node, look up `doc_frequency`
   from the term info table (one read per term, no posting list decoding
   needed). Compute estimated selectivity:
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
3. **Term Resolution**: For each term/prefix/suffix node per column:
   - Term: forward FST lookup -> single posting list
   - Prefix: forward FST prefix iterator -> multiple posting lists -> union
   - Suffix: reverse FST prefix iterator (on reversed suffix) -> term_ordinals -> posting lists -> union
4. **Page-Level Candidate Selection** (approximate):
   Doc_ids are segment-local — they cannot be intersected across
   segments (see § 7, Term-Range Splitting). The approach depends on
   whether query terms land in the same or different segments:

   **Same segment** (all terms in one segment's term range):
   - AND: intersect `doc_id` lists directly (sorted u32 merge — fast)
   - OR: union `doc_id` lists directly
   - Resolve surviving doc_ids via that segment's doc table

   **Different segments** (terms span split siblings):
   - For each term, resolve its posting list's doc_ids through the
     owning segment's doc table → set of `(file, row_group, page)`
     page-location tuples
   - AND: intersect page-location sets (HashSet intersection)
   - OR: union page-location sets
   - No doc_id compatibility needed — intersection happens on
     resolved locations

   NOT: **skipped** at this stage (see NOT semantics above).
   Result: candidate page locations per column (may contain false
   positives).

5. **File Grouping**: Group candidate page locations by
   `(file, row_group)`, deduplicate, and sort by `first_row_index`
   within each group. This produces one work item per
   (file, row_group) pair, each containing the sorted row ranges to
   read. Grouping enables coalesced reads per file instead of
   scattered per-page reads.
6. **Cross-Column Intersection** (if multi-column query):
   - Intersect/union row ranges across columns using `first_row_index`
     and `row_count` to find overlapping intervals
7. **RowSelection Construction**: For each (file, row_group) work item,
   build a `RowSelection` from its sorted row ranges. Adjacent or
   overlapping ranges are merged. The `RowSelection` tells the Parquet
   reader exactly which rows to decode and which to skip.
8. **Page Fetch**: read candidate rows using `ParquetRecordBatchReaderBuilder`
   with `RowSelection` + `ProjectionMask` (include all searched columns)
9. **Row-Level Verification** (ILIKE pre-filter + tokenization):
   Page-level posting lists are approximate — a candidate page with 8192
   rows may contain only a few actual matches. To avoid tokenizing every
   row (which dominates CPU for large pages or long text fields), apply
   a two-stage filter:

   a. **ILIKE pre-filter**: For each query term, run Arrow's SIMD
      `ilike` kernel on the raw string column — case-insensitive
      substring check, no tokenization. AND/OR the resulting BitArrays
      across terms. This cheaply discards the vast majority of
      non-matching rows (SIMD scan, no Unicode normalization, no
      allocation per token).
   b. **Tokenize survivors only**: For rows passing the ILIKE filter,
      tokenize and evaluate the full boolean AST (including NOT
      clauses). Discard rows that don't match. ILIKE is a superset
      filter (substring, not token-boundary), so a few false positives
      reach this stage, but the count is small.

   This is the same ILIKE pre-filter used in the brute-force path,
   applied to the indexed path's verification step. For a page where 3
   out of 8192 rows match, tokenization runs on ~5-10 rows (ILIKE
   survivors) instead of 8192.

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

Used when indices are unavailable (column has not been fully indexed yet or
some files are not covered). The query path identifies unindexed files by comparing
the column's `last_indexed_snapshot` against the current catalog snapshot via
`catalog.files_added_between(last_indexed, current_snapshot)`.

For indexed files: use the indexed path (page-level candidates -> row
verification).

For un-indexed files, the brute-force path uses two-pass projection:

1. **Pass 1 — stream only the searched column**: Read full row groups sequentially
   (large I/O, not page-random). ILIKE pre-filter (SIMD substring check) to cheaply
   discard non-candidate rows. Tokenize survivors, evaluate query terms. Record
   matching row indices.
2. **Pass 2 — read remaining projected columns for matches only**: Build
   `RowSelection` from matching row indices. Read other columns with
   `ProjectionMask` + `RowSelection`. Score matches with BM25.

Two-pass avoids reading non-searched columns for the ~99% of rows
that don't match. Sequential column streaming is also cheaper on
object stores than many small page reads (fewer requests, better
throughput).

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
| `search.and` / `search.or` | object[] | yes (cross-column) | — |
| `search.multi_match` | string | yes (multi-match) | — |
| `search.columns` | string[] | yes (multi-match) | — |
| `select` | string[] | no | all columns |
| `limit` | integer | no | unlimited |
| `score` | boolean | no | false |
| `selectivity_threshold` | float | no | 0.0 |

Example:
```
b'{"table":"events","search":{"column":"description","match":"error timeout","operator":"and"},"select":["timestamp","service"],"limit":10,"score":true}'
```

Agents implementing Flight client/server MUST use these exact field
names. The REST handler and Flight handler parse the same JSON shape.

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

## 10. Data Lake Catalog Client

LakeSearch delegates file inventory to the data lake's own catalog. A
trait defines the interface:

```rust
/// A snapshot identifier in the data lake.
/// For DuckLake this is a BIGINT snapshot_id. Stored as a string for
/// format-agnostic representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId(pub String);

/// A data file tracked by the data lake.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataFile {
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
}

/// Read-only interface to a data lake's catalog metadata.
///
/// One instance per table. The table identity is provided at
/// construction time (e.g., `DuckLakeCatalog::new(path, table_name)`).
///
/// Implementations query the data lake's own metadata (DuckLake catalog
/// tables, Iceberg manifests, Delta log, etc.). LakeSearch never lists
/// object storage for file discovery.
///
/// The trait is intentionally minimal — only operations that all major
/// lake formats (DuckLake, Iceberg, Delta Lake) can implement natively.
/// LakeSearch handles diff logic and liveness analysis itself using
/// these primitives.
#[async_trait]
pub trait DataLakeCatalog: Send + Sync {
    /// Return the current (latest) snapshot ID.
    async fn current_snapshot(&self) -> Result<SnapshotId>;

    /// Return all data files active at the given snapshot.
    async fn files_at_snapshot(
        &self,
        snapshot: &SnapshotId,
    ) -> Result<Vec<DataFile>>;

    /// Return data files added in the range (from, to].
    /// "Added" means the file first became active after `from` and
    /// at or before `to`.
    ///
    /// All methods that accept a SnapshotId must return a clear error
    /// if the snapshot does not exist (expired, invalid, etc.).
    /// No separate validation method — callers rely on these methods
    /// to fail fast with a descriptive error.
    async fn files_added_between(
        &self,
        from: &SnapshotId,
        to: &SnapshotId,
    ) -> Result<Vec<DataFile>>;

    /// Check which of the given paths are active at the current snapshot.
    /// Returns the subset of `paths` that exist as live files.
    ///
    /// Used by the query path for stale filtering — only the files
    /// referenced by candidate segments need to be checked, not the
    /// entire file inventory. This avoids materializing the full file
    /// list on the query hot path.
    ///
    /// DuckLake: targeted SQL WHERE path IN (...) — fast.
    /// Iceberg/Delta: streams manifests/log but only O(paths.len())
    /// memory instead of O(all_files).
    async fn check_files_live(
        &self,
        paths: &[String],
    ) -> Result<HashSet<String>>;
}
```

### Cross-Format Compatibility

The trait is designed around operations that all major lake formats
support natively. No format-specific concepts (DuckLake's
`begin_snapshot`/`end_snapshot` columns, Iceberg's manifest entry
status, Delta's Add/Remove actions) leak into the interface.

| Operation | DuckLake | Iceberg | Delta Lake |
|-----------|----------|---------|------------|
| `current_snapshot()` | `SELECT MAX(snapshot_id) FROM ducklake_snapshot` | `table_metadata.current_snapshot_id` | `DeltaTable::get_latest_version()` |
| `files_at_snapshot(S)` | `WHERE begin_snapshot <= S AND (end_snapshot > S OR IS NULL)` | Read manifest list + manifests at snapshot S, filter `status != DELETED` | `load_version(S)` + `get_file_uris()` |
| `files_added_between(A, B)` | `WHERE begin_snapshot > A AND begin_snapshot <= B` | Walk snapshot chain A->B, collect manifest entries with `status = ADDED` | Read commit entries A+1..B from `_delta_log/`, collect Add actions |
| `check_files_live(paths)` | `WHERE path IN (...) AND active` — targeted SQL | Stream manifests, check against input set — same I/O, O(paths) memory | Stream log, check against input set |

**Stale file detection**: Compact uses `files_at_snapshot(current)` to
build the full live set (batch operation, memory is acceptable). Query
uses `check_files_live(candidate_paths)` to check only the files from
candidate segments — avoids materializing the full file list on the
hot path.

**Why no `files_removed_between()`**: While all three formats *can*
provide this (DuckLake: `end_snapshot` column; Iceberg: `DELETED`
manifest entries; Delta: Remove actions), the semantics vary. Iceberg's
`DELETED` entries may reference delete files (position/equality deletes),
not just compaction. Delta's Remove actions have a `data_change` flag
that distinguishes real deletes from OPTIMIZE. Rather than papering over
these differences in the trait, LakeSearch uses `files_at_snapshot` for
liveness checks — simpler, correct across all formats, and avoids
misinterpreting format-specific remove semantics.

### LakeSearch's Diff Logic (Not the Catalog's Job)

The catalog provides file listing primitives. LakeSearch is responsible
for the business logic:

- **New files to index**: `files_added_between(last_indexed, target)`
- **Stale files to clean up (compaction)**: files referenced by existing
  segments whose path is NOT in `files_at_snapshot(current_snapshot)`.

This separation keeps the catalog trait portable across formats and the
business logic in LakeSearch where it can be tested independently.

### Implementation Notes Per Format

**DuckLake** (via `duckdb` crate): All operations are single SQL queries
against `__ducklake_metadata_{catalog}.ducklake_data_file`. Simplest
implementation. Best for local development and CLI usage.

**Iceberg** (via `iceberg-rust` crate or REST catalog): Requires reading
Avro manifest files from object storage. More I/O than DuckLake but no
external database. `files_added_between` walks the snapshot chain via
`parent_snapshot_id` and collects `ADDED` entries from manifests written
at each intermediate snapshot.

Iceberg catalog latency can be mitigated with **manifest caching**.
Manifest files are immutable once written (same property as our segment
files). The caching strategy:

1. **Cache manifest files by path** — LRU keyed by manifest path.
   Once read, never invalidated.
2. **Cache the current manifest list** — re-read only when the
   snapshot advances. Track the current snapshot ID; if unchanged,
   reuse the cached manifest list.
3. **Incremental updates** — when the snapshot advances, read the new
   manifest list (small), diff against the old one, read only the
   new/changed manifests. Iceberg appends typically add 1-2 new
   manifests per snapshot.

With this caching, steady-state `check_files_live` and
`files_added_between` calls are in-memory scans over cached manifests —
no S3 reads. The cost model becomes comparable to DuckLake's local SQL
after the initial cold load.

**Delta Lake** (via `deltalake` / delta-rs crate): Reads JSON commit
entries from `_delta_log/`. `files_added_between` reads commit files
in the version range and collects Add actions. Native Rust crate with
good `object_store` integration.

### DuckLake Implementation (First Target)

DuckLake stores file metadata in `ducklake_data_file` with
`begin_snapshot` / `end_snapshot` columns — exactly what we need.

```rust
pub struct DuckLakeCatalog {
    conn: Mutex<duckdb::Connection>,
    catalog_name: String,
}
```

**Thread safety**: `duckdb::Connection` is `!Send`. The `Mutex`
serializes catalog calls across threads. This is fine because DuckLake
catalog calls are local SQL queries that return in microseconds — the
mutex is held only for the duration of one SQL query, not during
segment loading or Parquet reads. Contention is negligible relative
to actual query work. Iceberg and Delta catalogs use async
`object_store` APIs and don't have this constraint.

**Setup**:
```rust
impl DuckLakeCatalog {
    pub fn new(ducklake_path: &str, data_path: &str) -> Result<Self> {
        let conn = duckdb::Connection::open_in_memory()?;
        // Load extension
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
        // Attach the DuckLake catalog
        conn.execute_batch(&format!(
            "ATTACH 'ducklake:{}' AS lake (DATA_PATH '{}');",
            ducklake_path, data_path
        ))?;
        // CRITICAL: Disable inlined data. DuckLake can inline small
        // writes into the metadata DB instead of writing Parquet files.
        // We need ALL data in Parquet so we can index it.
        conn.execute_batch(
            "CALL lake.ducklake_set_option('lake', 'inline_data', false);"
        )?;
        Ok(Self { conn: Mutex::new(conn), catalog_name: "lake".into() })
    }
}
```

**Query implementations**:

```sql
-- current_snapshot():
SELECT snapshot_id FROM ducklake_snapshots('lake')
ORDER BY snapshot_id DESC LIMIT 1;

-- files_at_snapshot(snapshot_id):
-- Files active at a given snapshot: begin_snapshot <= snap
-- AND (end_snapshot > snap OR end_snapshot IS NULL)
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

DuckLake's metadata catalog is a single DuckDB file (e.g.,
`./events.ducklake`). This works well for **local/CLI usage** — you run
LakeSearch next to your application that reads and writes the data lake,
and they share the same DuckDB file on the local filesystem.

It does **not** work well for **distributed/server deployments**. A remote
query service would need access to the same DuckDB file as the writer
process. DuckDB files cannot be shared across machines via object storage
(they're mutable, and object stores don't support in-place mutation). You
would need the query service co-located with the writer, or a shared
filesystem.

This is a known limitation of the DuckLake implementation specifically,
not of LakeSearch's architecture. Iceberg (REST catalog) and Delta Lake
(transaction log on object storage) don't have this constraint — their
metadata is either served via HTTP or stored as immutable files in object
storage, accessible from anywhere.

**DuckLake is for prototyping and local development.** For production
server deployments, use an Iceberg or Delta Lake catalog implementation.

### DuckDB-RS Usage

The `duckdb` Rust crate with `bundled` feature compiles DuckDB from
source. Extensions are loaded at runtime:

```rust
use duckdb::{params, Connection};

let conn = Connection::open_in_memory()?;
conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
conn.execute_batch(
    "ATTACH 'ducklake:./catalog.ducklake' AS lake (DATA_PATH './data/');"
)?;

// Query metadata
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

All we do is submit SQL queries. No special Rust API needed for DuckLake.

**Fallback**: If DuckDB-RS extension loading proves problematic, shell out
to the DuckDB CLI:
```rust
let output = Command::new("duckdb")
    .args([":memory:", "-json", "-c", &query])
    .output()?;
```


### Disabling Inlined Data

DuckLake can store small writes directly in the metadata database instead
of writing Parquet files. This breaks LakeSearch because we need all data
in Parquet files we can index. Disable on catalog setup:

```sql
CALL ducklake_set_option('lake', 'inline_data', false);
```

This must be set before any data is written, or existing inlined data
will not be in Parquet files. Document this as a hard requirement.

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

This gives us everything we need:
- **Files active now**: `end_snapshot IS NULL` (or `> current_snap`)
- **Files added since X**: `begin_snapshot > X`
- **Files removed since X**: `end_snapshot IS NOT NULL AND end_snapshot > X`

### What Compaction Looks Like in DuckLake

When a user calls `ducklake_merge_adjacent_files('lake')`:
1. Old small files get `end_snapshot` set to the new snapshot.
2. A new merged file gets `begin_snapshot` set to the new snapshot.
3. The merged file has `partial_max` set (it contains data from
   multiple original snapshots).

From LakeSearch's perspective:
- **Index** sees the new merged file as a new file (begin_snapshot > last_indexed).
- **Compact** sees the old files as stale (end_snapshot set, not in current snapshot).

This is exactly the append-and-compact pattern we designed for.

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
  meta|{table_id}|config              -> TableConfig
    Contains:
      indexed_columns: Vec<ColumnConfig>
        Each: { name: String, tokenizer: String, status: active|dropped }
      table_location: String               (data lake table path)
      catalog_uri: String                  (catalog connection string)

  meta|{table_id}|snapshot|{column}   -> ColumnSnapshotState
    Contains:
      last_indexed_snapshot: SnapshotId
    Per-column tracking. Each column advances independently.
    A newly added column has no entry (equivalent to last_indexed = None).
    In the happy path (all columns in sync), all entries have the same
    snapshot value and advance in lockstep.

  meta|{table_id}|corpus|{column}     -> CorpusStats
    Contains:
      total_rows: u64
      total_tokens: u64
    Per-column global stats for BM25. Updated atomically during index
    and compact.

SEGMENT INDEX
  seg|{table_id}|{column}|{max_term}|{segment_id}  -> SegmentEntry
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

    Sorted by (table, column, max_term). A range scan starting at
    seg|{table}|{col}|{query_term} finds all segments whose term
    range may contain the query term.
```

**Why this works for queries**: To find segments for term T, scan from
`seg|{table}|{col}|{T}` forward. The first key with `max_term >= T`
is the first candidate. Keep scanning until `min_term > T`. This is
O(matching segments), not O(all segments).

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

A `DbReader` sees a consistent SlateDB snapshot — either fully before
or fully after a transaction. All segment entries from one
index/compact commit appear atomically. A reader that started before
a commit continues seeing the old segment set for its entire query.

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
writer. Readers see a consistent point-in-time snapshot — either
before or after a metadata commit, never partial state. Vacuum's
grace period (default 1 hour) prevents it from deleting segment files
that a concurrent index/compact has written but not yet committed.

---

## 12. API Specifications

**Snapshot handling**: Index accepts an optional `target_snapshot` for
incremental backfill. All other APIs — query, compact,
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
    pub snapshot_advanced_to: SnapshotId,
}
```

**Algorithm**:

```
INDEX(table_id, target_snapshot?):

1. RESOLVE target snapshot:
     target = target_snapshot.unwrap_or(catalog.current_snapshot())

2. READ table config from SlateDB for indexed columns.
     Filter to columns with status = active.

3. For each active column, READ its snapshot pointer:
     key: meta|{table_id}|snapshot|{column}
     If not found -> last_indexed = None (first index or new column).

4. COMPUTE per-column file sets:
     Group columns by their last_indexed value (most will be the same).
     For each distinct last_indexed value:
       If last_indexed is None:
         files = catalog.files_at_snapshot(target)
       Else if last_indexed == target:
         files = []   (column is up to date)
       Else:
         files = catalog.files_added_between(last_indexed, target)

     This avoids redundant catalog calls — columns that are in sync
     share one call.

5. For each column that has new files:
     a. BUILD segment(s) from that column's file set:
        - Read each Parquet file's column pages via offset_index
        - Tokenize every row, build posting lists (page-level doc_ids)
        - Build doc table: doc_id -> (file, row_group, page, first_row, count)
        - Compute per-segment corpus stats (total_rows, total_tokens)
        - Build FST term dictionary
        - If segment > target_segment_size: split by term range

     b. WRITE segment file(s) to object storage:
        {table_path}/lakesearch/segments/{column}/{segment_id}.seg

6. COMMIT atomically (single SlateDB transaction):
     txn = db.begin(Snapshot)
     For each column with new segments:
       For each segment:
         txn.put(seg_key(table, col, seg.max_term, seg.id), encode(seg))
       // Update corpus stats
       old_stats = txn.get(corpus_key(table, col))
       txn.put(corpus_key(table, col), old_stats + seg_stats)
     // Advance all active columns' snapshot pointers to target
     For each active column:
       txn.put(snapshot_key(table, col), target)
     txn.commit()
     db.flush()   // ensure durable before returning

7. Return IndexResult { ... }
```

**Why all columns advance to target, not just the ones with work**: A
column with no new files (last_indexed == target, or files list empty)
still gets its pointer advanced. This keeps columns in lockstep after
the initial catch-up. If we only advanced columns that did work, a
column with no matches in the new files would fall behind for no reason.

**Backfill is not a separate operation.** When a new column is added
(see Column Lifecycle), its snapshot pointer doesn't exist. The next `index` call sees
`last_indexed = None` for that column, fetches all files via
`files_at_snapshot(target)`, and indexes them. Existing columns that
are already at target do nothing. After the call, all columns are in
sync.

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

**Idempotency**: Calling with the same or older target_snapshot produces
empty diffs at step 4. Snapshot pointers are re-written to the same
value. Safe to retry.

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
COMPACT(table_id):

--- Plan -----------------------------------------------------------

1. QUERY catalog for current liveness state:
     current_snap = catalog.current_snapshot()
     live_files = catalog.files_at_snapshot(current_snap)
     live_paths = { f.path for f in live_files }

2. SCAN all segments for this table from SlateDB:
     scan_prefix(seg|{table_id}|)

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
          - Union posting lists across segments.
          - For each doc_id in the unioned list, look up the doc table.
            If the doc table entry points to a stale Parquet file ->
            discard. Otherwise -> remap to a new dense doc_id.
          - If posting list is empty after filtering -> drop the term.
          - Recompute doc_frequency from surviving entries.
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
       txn.delete(seg_key(table, col, old_max_term, old_seg_id))
     // Replace merged segments with new ones
     For each merge group:
       For each consumed segment: txn.delete(old_key)
       For each new output segment: txn.put(new_key, entry)
     // Update global corpus stats per column
     For each affected column:
       old_corpus = txn.get(corpus_key(table, col))
       new_corpus = old_corpus - sum(consumed_segment_stats)
                               + sum(new_segment_stats)
       txn.put(corpus_key(table, col), new_corpus)
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
Parquet files." This naturally handles terms that disappear (posting list
becomes empty), doc_id renumbering, corpus stats recomputation, and FST
rebuild — all of which the merge already does. A single-segment merge
group (a stale segment alone in its tier) is a degenerate case: same
code path, no special case.

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

The query API, query language (match, multi_match, full boolean trees),
cross-column semantics, BM25 scoring, and execution pipeline are
defined in the Query Model section. The changes below describe how the query
*path* determines which files are indexed, which are unindexed, and which
are stale:

- **Per-column snapshot pointer**: Read from
  `meta|{table_id}|snapshot|{column}` in SlateDB. Each column may be
  at a different snapshot (e.g., during backfill).
- **Unindexed file detection**: Uses `catalog.files_added_between(
  last_indexed, current_snapshot)` to identify files not yet indexed.
- **Stale page filtering**: Uses `catalog.check_files_live(paths)`
  with only the candidate segments' file paths. Avoids materializing
  the full file list on the query hot path.

**Algorithm**:

```
QUERY(table_id, column, query_text, limit):

--- Step 1: Determine indexed vs unindexed files -------------------

1. GET current snapshot:
     snap = catalog.current_snapshot()

2. READ this column's snapshot pointer from SlateDB:
     key: meta|{table_id}|snapshot|{column}
     If not found -> last_indexed = None (column never indexed).

3. Determine unindexed files:
     If last_indexed == snap:
       unindexed_files = []            // fast path
     Else if last_indexed is None:
       unindexed_files = catalog.files_at_snapshot(snap)  // everything
     Else:
       unindexed_files = catalog.files_added_between(last_indexed, snap)

4. Stale filtering is deferred to after segment evaluation (step 7d).
     When last_indexed == snap, skip entirely (fast path).
     Otherwise, collect parquet file paths from candidate segments
     after step 6, then call catalog.check_files_live(paths) to get
     the live subset. This avoids materializing the full file list —
     only the files from candidate segments are checked.

--- Step 2: Indexed path (segment lookup) --------------------------

5. TOKENIZE query_text -> query_terms[]

6. For each query_term:
     SCAN SlateDB for candidate segments:
       Start: seg|{table_id}|{column}|{term}
       End:   seg|{table_id}|{column}|~
       Keep segments where min_term <= term <= max_term.

7. For each candidate segment:
     a. LOAD segment file (from cache or object storage).
     b. For each query_term:
        FST lookup -> term ordinal -> posting list offset -> decode doc_ids
     c. INTERSECT posting lists (AND) or UNION (OR).
     d. For each candidate doc_id:
        Look up doc table -> (parquet_file, row_group, page, first_row, count)
        FILTER: skip if parquet_file.path NOT IN live_paths
          (file was compacted away since last index)
        live_paths comes from catalog.check_files_live() called once
        after segment evaluation with all candidate segments' file paths.
     e. For surviving candidates:
        READ Parquet page.
        ILIKE pre-filter: SIMD substring check per query term to
          cheaply discard non-matching rows (no tokenization).
        Tokenize only ILIKE survivors, evaluate full boolean AST.
        Compute BM25 using global df (summed across segments) +
          global corpus stats from SlateDB.

8. Collect scored results from indexed path.

--- Step 3: Unindexed path (brute-force) ---------------------------

9. For each file in unindexed_files (two-pass projection):
     Pass 1 — stream only the searched column:
       Read full row groups sequentially (large I/O, not page-random).
       ILIKE pre-filter (SIMD substring check) to cheaply discard
       non-candidate rows. Tokenize survivors, evaluate query terms.
       Record matching row indices.
     Pass 2 — read remaining projected columns for matches only:
       Build RowSelection from matching row indices.
       Read other columns with ProjectionMask + RowSelection.
       Score matches with BM25.

   Two-pass avoids reading non-searched columns for the ~99% of rows
   that don't match. Sequential column streaming is also cheaper on
   object stores than many small page reads (fewer requests, better
   throughput).

10. Collect results from brute-force.

--- Step 4: Merge and return ---------------------------------------

11. MERGE indexed results + brute-force results.
12. SORT by BM25 descending. Apply limit.
13. Return results with stats.
```

For cross-column queries (designed in § 8, not yet implemented in the
query crate), the library runs steps 1-11 per clause, then joins
results by row identity `(file_path, row_index)`, applies the combine
strategy, sorts, and returns.

**Query during backfill**: If a column is mid-backfill (its
`last_indexed` is behind other columns), queries on that column
brute-force scan more files. This is correct — results are complete,
just slower until the backfill catches up. `unindexed_files_scanned`
in QueryStats tells the caller how much brute-force work was needed.

**Stale page filtering (step 7d)**: Between index and compact, some
segments reference Parquet files the data lake has already compacted.
Step 7d filters these at query time. Running compact promptly reduces
this overhead by rewriting the segments themselves.

**Performance tiers**:
- Best: fully indexed, no staleness -> no brute-force, no stale filtering.
- Typical: small unindexed tail -> most results from index, small scan.
- Worst: nothing indexed -> full brute-force. Same as no index at all.

**Optimization for the query path stale check**: Rather than calling
`files_at_snapshot` on every query (which reads manifest metadata), we
can skip stale filtering entirely when `last_indexed == snap`. When
they differ, the query needs to pay this cost. This is one reason to
call index frequently — it keeps queries fast.

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
VACUUM(table_id, grace_period = 1h):

1. LIST all segment files in object storage:
     {table_path}/lakesearch/segments/
   This is the only API that uses object storage LIST.
   Run sparingly (daily or weekly).

2. SCAN SlateDB for all live segment paths:
     scan_prefix(seg|{table_id}|)
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
`{table_path}/lakesearch/slatedb/compacted/` are cleaned up by
SlateDB's built-in compactor. Vacuum does not touch the `slatedb/`
directory.

---

## 13. Column Lifecycle

### Adding an Index

```
lakesearch add-index --table events --column description [--tokenizer default]
```

Writes an updated `TableConfig` to SlateDB with the new column added
(status = active). Does NOT create a snapshot pointer for the column —
the absence of a pointer means `last_indexed = None`.

The next `index` call sees the new column with no snapshot pointer,
fetches all files via `files_at_snapshot(target)`, and indexes them.
After the call, the new column is in sync with the others. This is
backfill — not a separate operation, just the natural behavior of
per-column snapshot tracking.

### Dropping an Index

```
lakesearch drop-index --table events --column description
```

Single atomic SlateDB transaction:
1. Update `TableConfig`: set column status to `dropped`
2. Delete all segment keys for that column:
   `scan_prefix(seg|{table_id}|{column}|)` -> delete each
3. Delete corpus stats: `delete(meta|{table_id}|corpus|{column})`
4. Delete snapshot pointer: `delete(meta|{table_id}|snapshot|{column})`

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
| SlateDB segment entries (from metadata reads) | Small | Read via `DbReader` snapshot — always consistent. Refresh periodically or on query if stale. |
| FSTs (from segment files) | Medium (typically KB-low MB per segment) | Immutable. Cache keyed by segment path. Evict via LRU. Highest-value cache items. |
| Doc tables (from segment files) | Small-medium (24 bytes x pages per segment) | Immutable. Cache keyed by segment path. Needed for every query that hits this segment. |
| Parquet footer + offset_index | Small | Immutable per file. Cache keyed by file path. Needed only for cross-column queries if doc table doesn't already have `first_row_index` (it does). |
| Posting list blocks | Large | Immutable. Optional — only cache hot blocks under LRU. |

### Staleness Safety

Segment files are **immutable files**. Once written, they never change. This means
cache invalidation is trivial:

- SlateDB metadata reads provide a consistent view of which segments are active.
  When a new segment entry appears (after index or compact commits), load the new
  segment data. Old cached segments remain valid as long as SlateDB still references
  them.
- When a SlateDB read shows a segment is no longer referenced, it can be evicted
  from cache. The underlying file will be cleaned up by vacuum.

The only race condition: a cached segment file could be vacuumed while still in cache.
Mitigate by ensuring vacuum's grace period is much longer than max query execution time.

### Cache Implementation

Use `moka` LRU cache with a configurable memory budget (default 256MB).
Priority: FSTs (highest hit rate), doc tables (needed for every query),
posting list blocks (largest, lowest priority).

### Cache Correctness

Cache correctness depends ONLY on the SlateDB snapshot taken at query
start, never on cache freshness:
- Segment files are immutable, addressed by unique paths (UUIDv7).
  Cache key = path. No ETag/version needed.
- Parquet metadata is immutable per file. Same logic.
- SlateDB `DbReader` snapshot determines which segments are consulted.
  Cache only affects whether bytes come from memory or object storage.
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

// -- Segment reading (query service consumes these) --

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

// -- Posting list codec --

pub fn encode_posting_list(doc_ids: &[DocId]) -> Vec<u8>;
pub fn decode_posting_list(data: &[u8]) -> Vec<DocId>;

// -- Boolean operations on sorted doc_id arrays --

pub fn intersect(a: &[DocId], b: &[DocId]) -> Vec<DocId>;
pub fn union(a: &[DocId], b: &[DocId]) -> Vec<DocId>;
pub fn difference(a: &[DocId], b: &[DocId]) -> Vec<DocId>;

// -- Tokenizer --

pub fn tokenize(text: &str) -> Vec<String>;

// -- BM25 (stateless math) --

pub fn bm25_score(tf: f32, df: u32, dl: u32, avg_dl: f32, n: u64) -> f32;

// -- Metadata types (SlateDB key-value model) --

pub struct TableConfig { .. }           // indexed_columns, table_location, catalog_uri
pub struct ColumnSnapshotState { .. }   // last_indexed_snapshot
pub struct CorpusStats { .. }           // total_rows, total_tokens
pub struct SegmentEntry { .. }          // segment_path, min_term, max_term, size_bytes, doc_count, parquet_files, ...
```

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
   queue, dispatches to rayon for ILIKE pre-filter + tokenization +
   verification + scoring. Bounds in-flight CPU work to available
   threads. Uses `tokio::select!` with biased priority to drain
   completed work before pulling new items.
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
                            query, DataLakeCatalog trait + DuckLake impl,
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
                          Commands: init, add-index, drop-index, index,
                                   compact, vacuum, query.
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

# -- Benchmark 2: Common term (low selectivity) --
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

# -- Benchmark 3: Multi-term AND (high selectivity) --
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

# -- Benchmark 4: Prefix search --
def indexed_prefix():
    reader = client.do_get(flight.Ticket(
        b'{"table":"events","search":{"column":"description","match":"conn*"}}'
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
index(table_id="events", target_snapshot=Some(1))

1. target = snapshot 1
2. Config: one column "description"
3. message: no snapshot pointer -> last_indexed = None
4. catalog.files_at_snapshot(1) -> 100 files
5. Build segments for "description" column
   -> 10 segments of ~5MB each
6. Write to s3://lake/events/lakesearch/segments/description/*.seg
7. SlateDB txn:
   - PUT seg|events|description|{max_term_1}|{seg_1} -> ...
   - ... (10 entries)
   - PUT meta|events|corpus|description -> { total_rows: 100000, ... }
   - PUT meta|events|snapshot|description -> { last_indexed: 1 }
   - COMMIT + flush
```

### Day 2: Incremental Writes

User inserts 5K rows in 5 new files. Snapshot = 2.

```
index(table_id="events")    // no snapshot -> uses current = 2

1. target = 2
2. message: last_indexed = 1
3. catalog.files_added_between(1, 2) -> 5 new files
4. Build 1 small segment (~500KB)
5. SlateDB txn:
   - PUT seg|events|description|{max_term}|{seg_11} -> ...
   - UPDATE corpus stats
   - PUT meta|events|snapshot|description -> { last_indexed: 2 }
   - COMMIT + flush
```

### Day 3: Data Lake Compaction

User runs `ducklake_merge_adjacent_files('lake')`. DuckLake merges the
100 original files into 10 big files. Snapshot = 3. The 5 Day-2 files
are untouched.

```
Step 1: index(table_id="events")    // indexes new compacted files

1. target = 3
2. message: last_indexed = 2
3. catalog.files_added_between(2, 3) -> 10 new merged files
4. Build segments for the 10 new files
5. Commit: new segments + message snapshot -> 3

Step 2: compact(table_id="events")  // clean up stale references

Plan:
1. current_snap = 3
   live_files = catalog.files_at_snapshot(3) -> 10 merged + 5 day-2 files
2. Scan segments -> 10 original + 1 day-2 + new from step 1
3. 10 original segments reference the 100 pre-compaction files
   -> all stale (fully_stale)
4. Build merge groups for remaining segments by size tier

Execute:
5. Delete 10 fully_stale segments, merge groups as needed
6. Commit
```

### Day 4: Add a New Column (Backfill)

User wants to index the `description` column too.

```
add-index(table_id="events", column="description")
  -> Updates config in SlateDB. No snapshot pointer for description.

index(table_id="events")

1. target = 3 (current snapshot, unchanged since day 3)
2. Config: message (active), description (active)
3. Snapshot pointers:
   - message: last_indexed = 3 -> up to date, no work
   - description: no pointer -> last_indexed = None
4. Catalog calls:
   - message: files_added_between(3, 3) -> empty (or skip entirely)
   - description: files_at_snapshot(3) -> 15 files (10 merged + 5 day-2)
5. Build segments for description from all 15 files
6. SlateDB txn:
   - PUT seg|events|description|{max_term}|{seg} -> ... (new segments)
   - PUT meta|events|corpus|description -> { total_rows: ..., ... }
   - PUT meta|events|snapshot|description -> { last_indexed: 3 }  // unchanged
   - PUT meta|events|snapshot|description -> { last_indexed: 3 }
   - COMMIT + flush

Both columns now in sync at snapshot 3.
```

### Query During Backfill

If a user queries `description` before the backfill index runs:
- description has no snapshot pointer -> last_indexed = None
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
- **This gives us the `QueryTerm` canonical representation** that the
  builder API and QueryPlan depend on.

**From `docs/v2-design`** (cherry-pick only):
- Segment merge primitive (`merge.rs`). Needed for Phase 4.

**Query API builders and LakeSearch struct**:
- Implement `LakeSearch` struct with `open` (read-only, `DbReader`)
  and `open_mut` (read-write, `Db`). Parses catalog URI, resolves
  table path, opens SlateDB. User never sees internals.
- Implement `QueryRequest::match_text()`, `multi_match()`,
  `multi_column()` builders that produce the internal `QueryPlan`.
- `QueryPlan` and `ColumnPlan` structs (private to the library).
- Wire builders into REST handler (parse JSON → builder → QueryPlan),
  CLI (parse flags → builder → QueryPlan), and Flight ticket parsing.
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
- Define response stat structs upfront: `QueryStats` (segments
  consulted/pruned, pages scanned, rows verified, unindexed files
  scanned, stale pages filtered), `CompactResult` (merged, stale
  cleaned, created), `IndexResult` (segments created, files indexed
  per column), `VacuumResult` (files deleted). Wire into responses
  as operations are built.

**Milestone**: Single `lakesearch` crate with wildcard support, query
builder API, `LakeSearch` struct, canonical `QueryPlan`, segment
caching, structured tracing, and response stats. All existing tests
pass. Merge primitive available for Phase 4.

### Phase 1: Core Infrastructure

Foundation libraries inside the `lakesearch` crate.

**SlateDB integration**:
- Internal `MetadataStore` wrapper encapsulating the key layout:
  encode/decode `TableConfig`, `ColumnSnapshotState`, `CorpusStats`,
  `SegmentEntry` to/from `\x00`-separated byte keys and bincode
  values.
- Methods: `get_config`, `get_column_snapshot`, `put_column_snapshot`,
  `get_corpus_stats`, `put_corpus_stats`, `put_segment`,
  `delete_segment`, `scan_segments(table, column)`,
  `scan_all_segments(table)`.
- Transaction wrapper: `begin_txn()` → `MetadataTxn` with buffered
  puts/deletes, `commit()`, `flush()`.
- Internal to `LakeSearch` — users never interact with it directly.
- Tests: round-trip each key type, transaction atomicity, scan range.

**DataLakeCatalog trait**:
- `DataLakeCatalog` trait: `current_snapshot`, `files_at_snapshot`,
  `files_added_between`, `check_files_live`. Types: `SnapshotId`,
  `DataFile`.
- `DuckLakeCatalog` (behind `ducklake` feature flag):
  - Opens `duckdb::Connection`, installs/loads ducklake extension,
    attaches catalog, disables inline data.
  - Each trait method maps to one SQL query against
    `__ducklake_metadata_{catalog}.ducklake_data_file`.
  - `table_id` resolution from table name.
  - Clear errors for invalid snapshots, missing tables.
- `MockCatalog` for unit tests (no DuckDB dependency).
- Tests: mock catalog exercises all four methods. DuckLake integration
  test (`#[ignore]`).

**LakeSearch operations — init, add_index, drop_index**:
- `LakeSearch::init(catalog_uri, table_name, columns)`:
  - Parses catalog URI, constructs catalog, resolves table path.
  - Creates SlateDB at `{table_path}/lakesearch/slatedb/`.
  - Writes `meta|{table_id}|config`. Idempotent.
- `ls.add_index(column, tokenizer)` (requires `open_mut`):
  - Reads config, appends column (status=active), writes back.
  - No snapshot pointer created (backfill happens on next `index`).
- `ls.drop_index(column)` (requires `open_mut`):
  - Single transaction: update config (status=dropped), delete all
    segment keys, corpus stats, and snapshot pointer for that column.
- CLI wrappers: `lakesearch init`, `lakesearch add-index`,
  `lakesearch drop-index` call through `LakeSearch` struct.
- Tests: init → verify config, add-index → verify config, drop-index
  → verify segments/stats/pointer deleted.

**Milestone**: `LakeSearch::init()` → `add_index()` → `drop_index()`
works against DuckLake, backed by SlateDB. All metadata operations
tested with mock catalog.

### Phase 2: Query Path Improvements

Fix the query layer before building new features. Items within this
phase are independent — can be done in any order or in parallel.

**ILIKE pre-filter on indexed verification path**:
- Add ILIKE pre-filter to `verify_batch` (indexed candidates): run
  Arrow's `ilike_utf8` kernel per query term, AND/OR the boolean
  masks, filter the RecordBatch before tokenization.
- Expected: tokenization on ~10 rows instead of 8192 per candidate
  page. Orders-of-magnitude CPU reduction.
- Tests: same results with and without ILIKE. Benchmark tokenization
  call count.

**Global df(t) aggregation for BM25**:
- After loading candidate segments, sum `doc_frequency` across all
  segments per query term. Read global `total_rows` and `total_tokens`
  from SlateDB corpus stats.
- Thread `global_df` and `global_corpus_stats` through
  `SharedQueryContext` to scoring.
- Tests: two segments with overlapping terms → verify scores use
  summed df. Golden test with known inputs.

**Two-pass brute-force projection**:
- Pass 1: stream only the searched column. ILIKE + tokenize. Collect
  matching row indices.
- Pass 2: build RowSelection from matches, read other projected
  columns for matches only.
- I/O reduction: ~1% of reads for non-searched columns vs 100%.
- Tests: projected columns correct for matches, absent for non-matches.

**Query server on SlateDB**:
- `LakeSearch::open()` (read-only) replaces `MetadataCache`.
- Query planning reads segment entries from SlateDB via
  `scan_segments(table, column)` instead of walking manifest lists.
- Stale filtering: `catalog.check_files_live(candidate_paths)`.
- Unindexed detection: `catalog.files_added_between(last_indexed,
  current)`.
- Remove: `read_current`, `read_metadata`, manifest list loading.

**Segment caching update**:
- `ObjectCache` caches segment file bytes by path (immutable).
- Remove manifest/manifest-list caching.
- Snapshot `DbReader` at query start for consistent segment metadata.
- Parquet metadata caching unchanged.

**Milestone**: Query path reads from SlateDB + catalog. BM25 uses
global df. ILIKE pre-filter on indexed path. Two-pass brute-force.
All existing query tests pass with new backend.

### Phase 3: Indexer Update

Port the indexer to v3 architecture and pipeline it.

**Port to SlateDB + catalog**:
- `ls.index(IndexRequest)` internally:
  - Resolves target snapshot from catalog (or uses provided).
  - Reads config and per-column snapshot pointers from SlateDB.
  - Groups columns by `last_indexed`, one catalog call per distinct
    value.
  - Builds segments per column (core logic unchanged).
  - Writes segments to `{table_path}/lakesearch/segments/`.
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

**Missing column handling**:
- Skip files missing the indexed column (log debug). Snapshot pointer
  still advances. No terms contributed.

**Backfill verification**:
- Integration test: create table with column A, index, add-index
  for column B, run index. Column A does no work, column B indexes
  all files. Both pointers at target.
- Test incremental backfill: index to snapshot 5, then 10.

**Milestone**: `init` → `add_index` → `index` → `query` works
end-to-end with DuckLake + SlateDB. Indexer pipelined. Backfill
tested.

### Phase 4: Compaction + Term-Range Splitting

This is the most complex phase.

**Port and extend merge primitive**:
- Merge primitive from `docs/v2-design`: merge-sort by term, union
  posting lists, concatenate doc tables, renumber doc_ids, rebuild FST.
- Add `stale_paths: &HashSet<String>` parameter. Skip stale doc table
  entries and their posting list doc_ids. Recompute doc_frequency and
  corpus stats from survivors. Omit dead terms from output FST.
- Tests: merge two segments, merge with stale paths, merge where all
  entries stale, single-segment rewrite.

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
- Tests: no-op compact, fully stale deletion, partial stale rewrite,
  size-tiered merge, full lifecycle with data lake compaction.

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

**CLI**: `lakesearch compact --table {name}`.

**Milestone**: Full index → compact → query lifecycle. Stale cleanup.
Term-range splitting. Cross-segment AND.

### Phase 5: Vacuum

**Vacuum implementation**:
- `ls.vacuum(VacuumRequest)` — read-only, uses `open` (not
  `open_mut`). LIST segments in object storage, diff against SlateDB,
  delete orphans older than grace period.
- UUIDv7 segment IDs prevent collision with successor writers.
- CLI: `lakesearch vacuum --table {name} [--grace-period 1h]`.
- Tests: orphan files deleted, live files preserved, grace period
  respected.

**Milestone**: Full lifecycle works — init, add-index, index, compact,
vacuum, query. All operations tested end-to-end.

### Phase 6: Benchmarks

**Criterion benchmark suite**:
- `benches/posting.rs`: encode/decode dense/sparse 10K.
- `benches/boolean.rs`: intersect/union/difference at various sizes.
- `benches/tokenizer.rs`: throughput MB/sec.
- `benches/segment.rs`: build time, cold read.
- `benches/e2e.rs`: rare term, common term, multi-term AND, index
  throughput.
- All use in-memory object store.

**Benchmark harness** (Python):
- Indexed vs brute-force via Arrow Flight.
- Test matrix: data sizes, selectivities, query types.
- Regression gate: >10% investigated.

**Milestone**: Performance validated. Benchmark suite in CI.

### Phase 7: Additional Catalog Implementations

Tracing, caching, and stats are built in Phase 0/1. This phase adds
support for production data lake formats beyond DuckLake.

**Iceberg catalog** (behind `iceberg` feature):
- `IcebergCatalog` using `iceberg-rust` crate. Reads table metadata
  from REST catalog or object storage.
- Manifest caching: LRU by path (immutable files), cache current
  manifest list (re-read on snapshot advance), incremental updates
  (diff old/new manifest list, read only new manifests).
- Steady-state `check_files_live` and `files_added_between` are
  in-memory scans over cached manifests. No S3 reads.
- Integration tests with `#[ignore]`.

**Delta Lake catalog** (behind `delta` feature):
- `DeltaCatalog` using `deltalake` crate. Reads `_delta_log/` commit
  entries. Checkpoint-based state reconstruction.
- Integration tests with `#[ignore]`.

**DataFusion integration**:
- Implement a DataFusion `TableProvider` that wraps `LakeSearch::query`
  as a scan node. A SQL query like
  `SELECT * FROM lakesearch('events', 'description', 'error timeout')`
  pushes the full-text predicate into LakeSearch and returns an Arrow
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

---

## 21. Design Decisions Summary

All design questions raised during the review process have been
resolved and inlined into their respective sections:

- **Key separator**: `\x00` (§ 11, key layout)
- **Stale filtering in merge**: `stale_paths` parameter on the merge
  primitive (§ 12b, compact API)
- **Corpus stats accuracy**: `token_count` added to doc table entries,
  24 bytes per entry (§ 3, segment file format)
- **Segment ID generation**: UUIDv7, time-sortable (§ 2, storage layout)
- **SlateDB per-table**: One instance per table (§ 2, storage layout)
- **Compact does not advance snapshot pointer**: Only index advances it
  (§ 12b, compact API)
- **Stale index queries**: Always return correct results, report
  `unindexed_files_scanned` in QueryStats (§ 12c, query path)
- **Query-time catalog latency**: Iceberg implementations should cache
  manifests; keeping index up to date eliminates catalog calls on the
  fast path (§ 10, Iceberg manifest caching)
- **`files_at_snapshot` memory at scale**: Query uses
  `check_files_live` with small input set; compact accepts full
  materialization as a batch cost. Users should run data lake
  compaction regularly for high-write tables (§ 10, catalog trait;
  § 12c, query path)
