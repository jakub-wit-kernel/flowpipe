# flowpipe: concurrent data-processing engine in C

[![CI](https://github.com/jakub-wit-kernel/flowpipe/actions/workflows/ci.yml/badge.svg)](https://github.com/jakub-wit-kernel/flowpipe/actions/workflows/ci.yml)
![License: MIT](https://img.shields.io/badge/license-MIT-blue)

flowpipe is a C11 library and CLI for parallel data processing on Linux.
Its parts:

- **`fp_queue`:** bounded MPMC queues in two implementations.
  - A **lock-free** Vyukov ring that blocks via a futex **eventcount**
  - A **mutex + condition-variable** baseline with the same interface
- **`fp_pool`:** a thread pool with backpressure, `wait_idle`, and drain or
  cancel shutdown that is safe against racing submitters.
- **`fp_pipeline`:**
  - a reader → workers → sink pipeline over a fixed set of chunks, so
    memory is bounded
  - optional **input-order output** via a reorder buffer
  - first-error and Ctrl-C propagation that never deadlocks
- **Workloads:** word frequency (per-worker hash tables, arena keys,
  lock-free parallel merge), an order-preserving transform, and parallel
  CRC-32 over many files.

Real output, run on the GPL-3 text that ships with Debian/Ubuntu:

```console
$ flowpipe wc -j 2 --top 3 /usr/share/common-licenses/GPL-3
345 the
221 of
192 to
flowpipe: 5688 words, 1038 distinct

$ flowpipe sum -j 2 /usr/share/common-licenses/GPL-2 /usr/share/common-licenses/GPL-3
4e46f4a1 18092 /usr/share/common-licenses/GPL-2
97673d00 35149 /usr/share/common-licenses/GPL-3

$ flowpipe upper -j 4 in.log -o out.log      # byte-identical to tr a-z A-Z
```

## Build and test

```bash
cmake -S . -B build && cmake --build build -j
(cd build && ctest --output-on-failure)      # unit, stress, pipeline, e2e

cmake -S . -B build-tsan -DFP_SANITIZE=thread && cmake --build build-tsan -j
(cd build-tsan && ctest)

./build/fpbench                              # queue / pool / pipeline tables
```

## Highlights

- **Lock-free queue, verified.** The stress test (7 shapes, up to 8
  producers × 8 consumers, capacity down to 2) checks exactly-once delivery
  and per-producer FIFO order for both implementations. Planted bugs (a
  broken CAS, a relaxed publish store) are caught by the test and by TSan
  respectively.
- **A benchmark-driven fix.** The first lock-free version *lost* to the
  mutex queue by up to 3.5x. Profiling the wake path showed a
  `FUTEX_WAKE` syscall on every push while a consumer slept. An epoch-based
  eventcount, where one CAS claims all waiters per syscall, turned that
  into a **1.4x–3.7x win across all shapes**.
- **Bounded memory by construction.** The pipeline owns exactly
  `max_inflight` chunks, and every queue can hold them all, so pushes never
  block and stages can't deadlock each other. Tests assert the peak
  in-flight count.
- **Robust shutdown and signals.**
  - The pool's submitter handshake means no task is lost when shutdown
    races with submit.
  - The CLI receives SIGINT in a dedicated `sigwait` thread.
  - Reads on pipes poll the cancel flag, so Ctrl-C works even while input
    is idle.

## Benchmarks (2-vCPU dev sandbox)

| queue shape (P × C) | lock-free | mutex | ratio |
|---|---|---|---|
| 1 × 1 | 14.96 M ops/s | 4.02 M ops/s | 3.72x |
| 2 × 2 | 12.27 M ops/s | 5.68 M ops/s | 2.16x |
| 4 × 4 | 8.97 M ops/s | 4.58 M ops/s | 1.96x |
| 4 × 1 | 5.71 M ops/s | 4.07 M ops/s | 1.40x |

Word count: 168 MiB/s on 1 worker, 365 MiB/s on 2. The thread pool with
4 workers on 2 CPUs is a case where the mutex queue wins: the
eventcount's wake-all wakes every idle worker. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §7 for the analysis. CI
publishes the same tables from a GitHub-hosted runner in each run's job
summary.

## Layout

```
include/flowpipe/  queue.h pool.h pipeline.h arena.h wordcount.h
src/               queue.c (lock-free + mutex), pool.c, pipeline.c, arena.c, wordcount.c
apps/flowpipe.c    CLI: wc, upper, sum
bench/fpbench.c    benchmark tables
tests/             queue stress, pool, pipeline, workloads (TAP)
scripts/e2e.sh     CLI against tr / python / zlib, plus SIGINT handling
```

## License

MIT. See [LICENSE](LICENSE).
