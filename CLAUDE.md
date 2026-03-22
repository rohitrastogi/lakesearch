# CLAUDE.md

## Project

LakeSearch: external full-text search indices for Parquet files in object storage.
Rust workspace with four crates: `lakesearch-core`, `lakesearch-indexer`,
`lakesearch-compactor`, `lakesearch-query`. See `DESIGN.md` for architecture.

## Readability

- Optimize for code that is easy to review, not clever.
- Prefer early returns and `?` over deeply nested control flow.
- Define variables as close as practical to where they are used.
- Public types and functions near the top of each module; private helpers lower.
- Non-obvious algorithms should include comments explaining invariants, tradeoffs,
  edge cases, or implementation details not clear from the code alone.
- Add `///` doc comments for all public types, functions, and modules when they
  define behavior or contracts not obvious from the type signature.
- Prefer `&str` over `String`, `&[T]` over `Vec<T>` in function parameters when
  ownership is not needed.
- Avoid `dyn Trait` unless dynamic dispatch is genuinely required. Prefer generics
  for static polymorphism.
- Use `tracing` for logging, not `println!` or `eprintln!` in application code.
  Use structured fields: `tracing::info!(segment_id = %id, term_count, "indexed segment")`.

## Design

- `lakesearch-core` is sync, pure, no I/O. Takes bytes in, gives bytes out.
  Service crates handle all async I/O and compose core's building blocks.
- No service crate depends on another service crate. They only share core.
- Before exposing a public API from core, review: are internal details leaked?
  Would a caller find the API obvious without reading the implementation?
- Make impossible states impossible to represent. Prefer enums with data over
  structs with related optional fields. If a value can be in one of N states,
  use an enum with N variants, not a struct with N optional fields.
- Use `serde` with strong types at external boundaries (JSON metadata, REST
  requests, Flight tickets). Validate and parse into domain types early.
  Internally, pass typed structs, not raw JSON or `serde_json::Value`.
- Use `thiserror` for error types in core (structured, matchable errors).
  Use `anyhow` in service crate binaries for top-level error handling.
  Do not mix: core should never depend on anyhow.
- Prefer pure functions where practical. Keep mutation localized to builders
  (`SegmentBuilder`, `PostingListBuilder`) and clearly stateful structs.
- Use `#[must_use]` on functions whose return value should not be ignored.
- Avoid `clone()` unless necessary. Prefer borrowing. When cloning is needed
  for async boundaries or thread transfer, document why.

## Error Handling

- Use `Result<T, E>` everywhere. No panics in library code (core) except
  for invariant violations that indicate programmer error.
- `unwrap()` / `expect()` are acceptable in tests and in cases where the
  invariant is documented and provably safe. Always use `expect("reason")`
  over bare `unwrap()`.
- Propagate errors with `?`. Add context at boundaries using `.context()`
  (anyhow) or by mapping to a domain error type.
- Catch errors at external boundaries: object storage calls, Parquet reads,
  network I/O. Let internal invariant violations fail loudly.

## Async and Concurrency

- All async I/O runs on tokio. All CPU-bound work runs on the rayon pool
  via `LakeRuntime::cpu()`. Never block tokio worker threads with CPU work.
- Use `tokio::sync::oneshot` to bridge rayon → tokio. Do not use
  `tokio::task::spawn_blocking` for CPU work (it grows an unbounded thread pool).
- For concurrent I/O (e.g., loading multiple segments), use `FuturesUnordered`
  or `tokio::task::JoinSet`. Bound concurrency where appropriate.
- Prefer structured concurrency: tasks should have clear ownership and
  cancellation semantics. Long-lived background work (compaction loop, cache
  refresh) should have explicit start/stop lifecycle.

## Testing

- Test behavior through public interfaces. Core's public API should be
  thoroughly tested; private helpers are tested indirectly.
- Write tests for code with meaningful branching, algorithmic complexity,
  or subtle invariants (posting list codec, boolean ops, BM25 math,
  segment round-trip, CAS retry logic).
- Each test should have one primary reason to fail.
- Integration tests: write Parquet files, index them, query, verify results.
  These are slow and should be marked with `#[ignore]` or run in a separate
  test target.
- Use `tempfile` for filesystem tests. Use `object_store::memory::InMemory`
  for storage tests — no real S3 calls in unit tests.

### Property-Based Tests (proptest)

Use `proptest` for components where correctness depends on handling arbitrary
inputs:

- **Posting codec**: generate random sorted `Vec<DocId>` → encode → decode →
  assert equal. Vary list length (0, 1, 128, 10K), density (dense sequential,
  sparse random), and value range.
- **Boolean ops**: generate two random sorted `Vec<DocId>` → compute
  intersect/union/difference → assert matches naive `BTreeSet` operations.
- **Segment round-trip**: generate random terms, doc_ids, doc table entries →
  build segment → read back → all fields match.
- **Tokenizer**: generate random Unicode strings → tokenize → verify invariants
  (all tokens lowercase, all tokens non-empty, all ≤256 bytes).

### Golden Tests

Checked-in fixtures for deterministic correctness checks:

- **Tokenizer**: `tests/fixtures/tokenizer_golden.json` — input strings and
  expected token lists. Catches accidental tokenizer behavior changes.
- **BM25**: `tests/fixtures/bm25_golden.json` — precomputed expected scores
  for known (tf, df, dl, avg_dl, N) inputs. Cross-checked against a reference
  implementation.

### Test Parquet Generator

A shared test utility (`lakesearch-core/src/test_utils.rs`, behind
`#[cfg(test)]` or a `test-utils` feature) that creates Parquet files with
known content patterns:

```rust
/// Create a Parquet file with deterministic content for testing.
/// `descriptions` cycle through the provided strings.
/// Page indices (offset_index) are always written.
pub fn write_test_parquet(
    path: &Path,
    num_rows: usize,
    page_size_rows: usize,
    descriptions: &[&str],
) -> Result<()>;
```

This ensures end-to-end tests are deterministic and self-contained — no
external test data files to manage.

## Benchmarks (criterion)

Every performance-sensitive component has `criterion` benchmarks. Benchmarks
are written alongside the code, not after. Run with `cargo bench` before
committing any change to a benchmarked component.

### Per-Component Benchmarks

**Posting codec** (`lakesearch-core/benches/posting.rs`):
- `posting/encode/dense_10k` — encode 10K dense sequential doc_ids
- `posting/encode/sparse_10k` — encode 10K sparse random doc_ids
- `posting/decode/dense_10k` — decode 10K dense doc_ids
- `posting/decode/sparse_10k` — decode 10K sparse doc_ids
- Log `bits_per_docid` as a custom metric (target: <2 bits dense, <8 sparse)

**Boolean ops** (`lakesearch-core/benches/boolean.rs`):
- `boolean/intersect/{size_a}_{size_b}` — 1K∩1K, 1K∩100K, 100K∩100K
- `boolean/union/{size_a}_{size_b}` — same size matrix
- `boolean/difference/{size_a}_{size_b}` — same size matrix

**Tokenizer** (`lakesearch-core/benches/tokenizer.rs`):
- `tokenizer/throughput` — MB/sec of English text tokenized

**Segment** (`lakesearch-core/benches/segment.rs`):
- `segment/build/{terms}_{docs}` — build time for N terms, M doc_ids
- `segment/read_term` — cold read: parse footer → load FST → lookup one term → decode posting list
- Log `segment_size_ratio` (segment bytes / indexed text bytes) as custom metric

**End-to-end** (`lakesearch-query/benches/e2e.rs`):
- `e2e/query/rare_term` — query a term appearing in <0.1% of rows
- `e2e/query/common_term` — query a term appearing in >10% of rows
- `e2e/query/multi_term_and` — 3-term AND query
- `e2e/index/throughput` — rows/sec indexing throughput
- Log `pruning_ratio` (candidate pages / total pages) and `false_positive_rate`
  (rows scanned / rows matched) as custom metrics

### Performance Regression Gate

If a benchmark regresses by more than 10% compared to the previous run,
investigate before committing. `criterion` reports statistical significance
automatically — trust its "regressed" / "improved" / "no change" classification.

## Workflow

- Create a feature branch for each logical piece of work (e.g.,
  `feat/segment-format`, `feat/posting-codec`, `feat/query-server`).
  Do not commit directly to `main`.
- Make one logical change per commit within the branch. Separate unrelated
  fixes, refactors, and feature work into different commits.
- When adding or changing production code, add or update tests in the same
  commit unless the change is purely mechanical.
- Before committing, always run `/simplify` to review changed code for
  reuse, quality, and efficiency. Apply any changes it suggests before
  committing.
- After `/simplify`, run `/codex-review` to get a second-opinion review
  from OpenAI Codex. Evaluate its feedback and fix any valid issues before
  committing.
- Run `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings`
  before each commit. Fix all errors before committing.
- Write clear commit messages: what changed and why.

### Pre-Commit Checklist

Run this sequence before every commit:

```
/simplify
/codex-review
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
cargo bench --all -- --quick
```

If any step fails or benchmarks regress >10%, fix before committing.
