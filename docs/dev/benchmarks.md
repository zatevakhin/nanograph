# nanograph Benchmarks

`nanograph` uses Criterion for repeatable performance measurement. The benchmark
suite lives in `/Users/andrew/code/nanograph/crates/nanograph/benches`.

This doc defines the lightweight in-repo benchmark framework used for quick
regression detection on a local developer machine.

## Goals

The in-repo benchmark suite is for:

- performance regression tracking
- understanding query, traversal, search, and result transport costs
- comparing commits or releases on a stable machine

The benchmark suite is not for:

- normal PR gating
- public cross-database comparisons
- release-grade or marketing-grade benchmark numbers

Heavy benchmarks and external baselines now belong in the separate benchmark
repo at `/Users/andrew/code/nanograph-bench`.

## Benchmark Layout

Current Criterion bench targets:

- `query_lookup`
  - `key_lookup`
  - `indexed_property_lookup`
- `traversal`
  - `one_hop`
  - `two_hop`
  - `filtered`
- `search`
  - `keyword_search`
  - `fuzzy_search`
  - `semantic_search`
  - `hybrid_search`
  - `semantic_signals_for_client`
- `result_transport`
  - `result_json_vs_arrow`

Helper code and scale selection live in
`/Users/andrew/code/nanograph/crates/nanograph/benches/common/mod.rs`.

## Fixture Policy

The suite uses only two fixture sources:

- synthetic generated datasets for lookup and traversal benchmarks
- checked-in examples for workflow-style search benchmarks:
  - `/Users/andrew/code/nanograph/examples/starwars`
  - `/Users/andrew/code/nanograph/examples/revops`

The suite should stay deterministic and offline.

## Scope

These are the only benchmark domains that should stay in this repo:

- `query_lookup`
- `traversal`
- `search`
- `result_transport`

These benches are intended to be:

- fast enough to run on a Mac
- useful for day-to-day engineering work
- suitable for coarse regression detection
- cheap enough to keep close to the main codebase

## Current Scale Profiles

The suite has two built-in modes today.

### Smoke Mode

Enabled with:

```bash
NANOGRAPH_BENCH_SMOKE=1
```

Current settings:

- Criterion:
  - sample size: `10`
  - warm-up: `250ms`
  - measurement: `1s`
- scales:
  - lookup: `10,000`
  - traversal: `2,000 nodes / 10,000 edges`

This is the intended local mode.

### Full Mode

Default when `NANOGRAPH_BENCH_SMOKE` is not set.

Current settings:

- Criterion:
  - sample size: `10`
  - warm-up: `1s`
  - measurement: `4s`
- scales:
  - lookup: `10,000` and `100,000`
  - traversal: `10,000 nodes / 50,000 edges`

This is the intended local full mode when you want more signal than smoke
without leaving your laptop.

## Recommended Workflows

### Local Developer Workflow

Use this before or during implementation work:

```bash
cargo check --benches -p nanograph
cargo bench -p nanograph --no-run
NANOGRAPH_BENCH_SMOKE=1 cargo bench -p nanograph --bench query_lookup
NANOGRAPH_BENCH_SMOKE=1 cargo bench -p nanograph --bench traversal
```

Add one more targeted bench only if you are changing that subsystem.

Keep this repo focused on quick local regression checks. For comparative
baselines and heavier workloads, use the separate benchmark repo.

## What To Compare

Use the benchmarks by subsystem:

- query planning / point lookup changes
  - `query_lookup`
- traversal engine changes
  - `traversal`
- search behavior changes
  - `search`
- result format and SDK transport changes
  - `result_transport`

## Legacy Perf Tests

The ignored perf tests under `/Users/andrew/code/nanograph/crates/nanograph/tests`
are legacy reference harnesses:

- `/Users/andrew/code/nanograph/crates/nanograph/tests/index_perf.rs`
- `/Users/andrew/code/nanograph/crates/nanograph/tests/json_output_perf.rs`
- `/Users/andrew/code/nanograph/crates/nanograph/tests/write_amp_perf.rs`

Do not expand those further. New benchmark work should go into Criterion under
`/Users/andrew/code/nanograph/crates/nanograph/benches`.

## Keep In Mind

- Run benchmark numbers on one stable machine class if you want history to mean
  anything.
- Do not treat laptop runs as release or marketing baselines.
- Keep the suite offline and deterministic.
- If a benchmark needs external baselines or heavier methodology, it belongs in
  `/Users/andrew/code/nanograph-bench`, not here.
