# CLAUDE.md

## Project

LakeSearch: external full-text search indices for Parquet files in data lakes.
Single `lakesearch` library crate with two thin binary wrappers:
`lakesearch-server` (query service) and `lakesearch-cli`.
See `DESIGN_CONSOLIDATED.md` for architecture, APIs, and implementation phases.

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

## Design Preferences

- **Don't expose implementation trees to users.** If there's a clean builder
  or struct API, use it. Internal ASTs, enum hierarchies, and plumbing types
  should be private. Users call `match_text()`, not
  `SearchExpr::Single(ColumnSearch { query: QueryNode::Match { ... } })`.
- **One path, not two.** Avoid "simple mode" and "advanced mode" code paths
  for the same operation. One canonical representation, one code path that
  handles all cases. Degenerate cases (single-column query, single-segment
  merge) should fall out naturally, not be special-cased.
- **All-or-nothing operations.** Index, compact, and other mutating operations
  are atomic. If one file in a batch fails, the whole batch fails. No partial
  commits, no per-item error recovery. Retry the batch.
- **Decisions, not options.** Pick one approach (UUIDv7, bincode, moka) and
  use it everywhere. Don't add configuration knobs for things that have a
  clear best answer. If a choice genuinely needs to vary, make it a trait
  or feature flag — not a runtime parameter.
- **Score is a boolean.** Don't add multi-mode enums when true/false suffices.
  Apply this principle broadly: if a parameter has two meaningful states, use
  `bool`, not an enum with two variants.

See `DESIGN_CONSOLIDATED.md` for full architecture. Key code-level rules:

- The library has two internal layers: **core** (sync, pure, no I/O) and
  **operations** (async, I/O, SlateDB, catalog). Both in the same crate.
- Make impossible states impossible to represent. Prefer enums with data over
  structs with related optional fields.
- Use `serde` with strong types at external boundaries (REST requests, Flight
  tickets). Validate and parse into domain types early.
  Internally, pass typed structs, not raw JSON or `serde_json::Value`.
- Use `thiserror` for error types in the core layer (structured, matchable).
  Use `anyhow` in the operations layer and binaries for top-level error
  handling. Core layer modules must not depend on anyhow.
- Prefer pure functions where practical. Keep mutation localized to builders
  (`SegmentBuilder`, `PostingListBuilder`) and clearly stateful structs.
- Use `#[must_use]` on functions whose return value should not be ignored.
- Avoid `clone()` unless necessary. Prefer borrowing. When cloning is needed
  for async boundaries or thread transfer, document why.
- SlateDB values are bincode-encoded. Keys use `\x00` separators.
- Segment IDs are UUIDv7. Cache with `moka`.

## Error Handling

- Use `Result<T, E>` everywhere. No panics in library code (core) except
  for invariant violations that indicate programmer error.
- `unwrap()` / `expect()` are acceptable in tests and in cases where the
  invariant is documented and provably safe. Always use `expect("reason")`
  over bare `unwrap()`.
- Propagate errors with `?`. Add context at boundaries using `.context()`
  (anyhow) or by mapping to a domain error type.
- Catch errors at external boundaries: object storage calls, Parquet reads,
  SlateDB operations, catalog calls. Let internal invariant violations fail
  loudly.

## Async and Concurrency

- All async I/O runs on tokio. All CPU-bound work runs on the rayon pool
  via `LakeRuntime::cpu()`. Never block tokio worker threads with CPU work.
- Use `tokio::sync::oneshot` to bridge rayon → tokio. Do not use
  `tokio::task::spawn_blocking` for CPU work (it grows an unbounded thread pool).
- For concurrent I/O (e.g., loading multiple segments), use `FuturesUnordered`
  or `tokio::task::JoinSet`. Bound concurrency where appropriate.
- Prefer structured concurrency: tasks should have clear ownership and
  cancellation semantics.
- For workloads that mix I/O and CPU (reading from object storage, then
  processing), separate the two into distinct stages connected by bounded
  channels. I/O tasks run on tokio, CPU work dispatches to rayon. Bound
  both I/O concurrency (semaphore) and in-flight CPU work (no more than
  available threads). See `lakesearch-query/src/query/pipeline.rs` for the
  reference implementation of this pattern.
- CPU work dispatched to rayon should be pure functions:
  `(input, context) → output`. No shared mutable state. This makes them
  safe to run in parallel with no synchronization.
- Use bounded channels between stages. Backpressure propagates naturally.
  On error, send it downstream and let channel drops handle cancellation.

## Testing

- Test behavior through public interfaces. The `LakeSearch` struct's methods
  should be the primary test surface for operations.
- Write tests for code with meaningful branching, algorithmic complexity,
  or subtle invariants (posting list codec, boolean ops, BM25 math,
  segment round-trip, SlateDB metadata round-trip, catalog diff logic).
- Each test should have one primary reason to fail.
- Use `MockCatalog` (in-memory file list with snapshot tracking) for unit
  tests. No DuckDB dependency in unit tests.
- Use `object_store::memory::InMemory` for storage tests — no real S3 calls.
- DuckLake integration tests use `#[ignore]` (require extension download).
- Integration tests: write Parquet files, index them, query, verify results.
  These are slow and should be marked with `#[ignore]` or run in a separate
  test target.
- Use `tempfile` for filesystem tests.

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

A shared test utility (behind `test-utils` feature) that creates Parquet files
with known content patterns:

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

## Benchmarks (criterion)

Every performance-sensitive component has `criterion` benchmarks. Benchmarks
are written alongside the code, not after. Run with `cargo bench` before
committing any change to a benchmarked component.

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
- Use [Conventional Commits](https://www.conventionalcommits.org/):
  `type(scope): description`. Types: `feat`, `fix`, `refactor`, `test`,
  `docs`, `chore`, `perf`, `ci`. Scope is the crate or module (e.g.,
  `feat(cli): add query command`, `fix(core): posting decode off-by-one`).
  Body explains *why*, not *what*.

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
