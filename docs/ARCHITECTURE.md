# flowpipe architecture

## 1. Requirements

| ID | Requirement |
|----|-------------|
| F1 | Bounded MPMC queue with blocking and non-blocking operations and a close-and-drain contract. |
| F2 | Thread pool with backpressure, wait-for-idle, and drain or cancel shutdown. |
| F3 | Pipeline: reader → N workers → sink. Optional input-order output, bounded memory, first-error propagation and cancellation. |
| F4 | Real workloads on top: word frequency (unordered), upper-casing (ordered), parallel file checksums (pool). |
| N1 | No lost or duplicated elements under any interleaving; per-producer FIFO. |
| N2 | No deadlock on any error, cancellation or shutdown path. |
| N3 | Memory bounded by configuration, not by input size or relative stage speeds. |
| N4 | Clean under ThreadSanitizer, ASan, UBSan and valgrind. |

## 2. Queues

Two implementations share one interface (`fp_queue`), so every higher
layer, test and benchmark can run on either.

### 2.1 Lock-free: Vyukov's bounded MPMC ring

Each cell holds a sequence number plus the payload. For ring position
`pos`:

| `cell.seq` | meaning |
|---|---|
| `pos` | free; the producer that claims `pos` may write it |
| `pos + 1` | full; the consumer that claims `pos` may read it |

**Push:**
1. Read `enq_pos`.
2. If `cell.seq == pos`, claim the slot with one CAS on `enq_pos`. If
   `cell.seq < pos`, the queue is full.
3. Copy the payload, then publish it with a **release** store of
   `seq = pos + 1`.

**Pop** is the mirror image. The consumer frees the cell for the next lap
with `seq = pos + capacity`.

Each operation is one CAS on a shared counter, plus acquire/release on a
cell that usually belongs to just one producer and one consumer.
`enq_pos`, `deq_pos` and the two eventcounts sit on separate cache lines,
so producers and consumers don't false-share.

The payload `memcpy` is a plain, non-atomic access. Its visibility comes
entirely from the release/acquire pair on `cell.seq`. ThreadSanitizer
models that, which is why the planted bug in §6 (a relaxed publish store)
is reported as a data race on exactly that `memcpy`.

### 2.2 Blocking without losing wake-ups: an epoch eventcount

A lock-free queue has no mutex to hang a condition variable on, so
blocking needs another mechanism. flowpipe uses an **eventcount**: one
64-bit word holding `(epoch << 32) | waiters`, with the epoch half doubling
as the futex word.

```
waiter:   key = prepare()      // waiters++, remember epoch; seq_cst fence
          if (try_pop()) { cancel(key); return; }
          wait(key)            // futex_wait(epoch, key): sleeps only if epoch == key
notifier: push(); notify()     // seq_cst fence; if waiters > 0:
                               //   CAS state -> (epoch + 1, 0); FUTEX_WAKE all
```

**Why it can't lose a wake-up.** The waiter registers, fences, then
re-checks. The notifier publishes, fences, then reads the registration.
With seq_cst fences on both sides (a Dekker handshake), at least one of
them sees the other:

- If the waiter's re-check sees the element, it doesn't sleep.
- If the notifier sees the registration, it bumps the epoch. Then
  `futex_wait(epoch, key)` either returns at once (the value changed) or is
  woken.

**Why epochs.** The first version used a plain waiter counter. The
benchmarks showed the lock-free queue *losing* to the mutex queue by up to
3.5x whenever threads slept. A producer issued a `FUTEX_WAKE` syscall on
every push until the sleeping consumer was scheduled and deregistered.

With epochs, the first notifier **claims** every registered waiter in one
CAS and makes a single syscall. Later pushes see `waiters == 0` and skip
the syscall. A waiter that leaves without sleeping deregisters only if its
epoch is still current; if a notifier already claimed it, there is nothing
to undo. After this change the lock-free queue went from losing most shapes
to winning **every** shape by 1.4x to 4.4x on the dev box (§7).

**Spin, then sleep.** Before registering, a thread polls up to 128 times
with `pause`, so short waits never reach the kernel.

**Wake-all.** The notifier wakes every claimed waiter, not one. It is
simple and safe, but it causes a thundering herd when many idle threads
wait on one queue: see the pool numbers in §7. A wake-one variant is listed
under future work.

### 2.3 Mutex baseline

A ring buffer, one mutex, and `not_full`/`not_empty` condition variables.
`pthread_cond_signal` wakes exactly one waiter.

### 2.4 Close contract

After `close()`:
- pushes fail with `-EPIPE`
- pops drain whatever remains, then fail with `-EPIPE`

A push racing with `close()` may or may not land, so callers close only
after their producers are done. The pool and the pipeline both enforce
this: the last producer closes, and the pool uses a submitter handshake
(§3).

## 3. Thread pool

- **Submit** registers in `pending` *before* pushing, so `wait_idle` never
  sees a premature zero. A full queue blocks the submitter (backpressure);
  `try_submit` returns `-EAGAIN` instead.
- **wait_idle** sleeps on a condition variable until `pending == 0`. The
  final decrement is `acq_rel`, so every task's effects happen-before the
  waiter's return. Called from one of the pool's own workers, it returns
  `-EDEADLK` instead of waiting on itself.
- **Shutdown without dropping work.** Submitters increment `submitters`,
  check `accepting`, push, then decrement. Shutdown:
  1. clears `accepting`
  2. waits for `submitters == 0`
  3. closes the queue

  No push can race with the close. A test runs 4 submitter threads against
  `shutdown()` and checks that every call either returned `-EPIPE` or had
  its task run.
- **Drain vs cancel.**
  - Drain runs everything already queued.
  - Cancel sets a flag (release) after storing the callback. Workers check
    it (acquire) on each pop and hand queued tasks to `on_cancel`, so
    their arguments can be freed.

  Running tasks always finish: there is no unsafe thread kill.
- **Signals.** Workers start with all signals blocked.

## 4. Pipeline

```
reader ─► work_q ─► worker × N ─► done_q ─► sink (calling thread)
  ▲                                           │
  └─────────────── free_q ◄───────────────────┘
```

- **Bounded memory (N3).** Exactly `max_inflight` chunks exist, allocated
  up front, and they circulate between the queues. If the reader gets
  ahead, it blocks on `free_q`. Peak memory is
  `max_inflight × (chunk_size + output size)` regardless of input size.
  Every queue's capacity is at least `max_inflight`, so pushes never block,
  which removes a whole class of deadlocks by construction.
- **Order restoration.** The reader stamps each chunk with `seq`. In
  ordered mode the sink parks finished chunks in
  `slots[seq % max_inflight]` and emits while `slots[next]` is filled.
  This can't collide: at most `max_inflight` chunks exist, so two in-flight
  chunks never share `seq % max_inflight`.
- **Errors and cancellation (N2).** The first error is recorded with a CAS
  and closes `free_q`, which unblocks a reader waiting for a free chunk.
  The reader stops and closes `work_q`. Workers keep draining but skip the
  work, so every chunk still reaches the sink and is released. The last
  worker closes `done_q`, and the sink's loop ends. All threads are joined
  before `fp_pipeline_run` returns the error. Cancellation is the same path
  with `-ECANCELED`.
- **Responsive cancellation on idle input.** For pipes, FIFOs and
  terminals, the reader waits with `poll()` in 100 ms slices and checks the
  cancel flag in between, so Ctrl-C works while input is idle. Regular
  files never block for long and keep a plain `read()`.

## 5. Workloads and memory management

- **Chunking at token boundaries.** The reader cuts each chunk after its
  last word or line boundary and carries the tail into the next chunk, so
  no word is split. A token longer than an entire chunk is the documented
  exception.
- **Word count without sharing.** Each worker counts into its own
  open-addressing hash table (FNV-1a, linear probing, load factor ≤ 0.7),
  indexed by the worker number the pipeline passes to `work()`. The hot
  path shares nothing and takes no locks.
- **Arena allocation.** Word keys live in a per-table bump allocator
  (`fp_arena`): one pointer bump per new key, everything freed at once,
  no per-key `malloc` and no fragmentation.
- **Parallel merge on the pool.** Worker tables are merged by **hash
  partition**: task *p* takes keys with `(hash >> 32) % P == p` from every
  table. No two tasks touch the same key, so the merge needs no locks
  either. The partition uses high hash bits and the tables index with low
  bits, so the two stay independent.
- **Top-K.** A bounded heap selects the K best (count desc, then word asc)
  in O(n log K) instead of sorting every distinct word.

## 6. Verification

| What | How |
|---|---|
| Queue correctness | Stress test across 7 shapes (1P1C up to 8P8C, capacity 2 to 1024) for both implementations: every element seen exactly once, and each consumer sees each producer's elements in order |
| Tests catch real bugs | Planted: claiming a slot with load+store instead of CAS gives lost elements, which the watchdog reports as a clear failure; publishing with a relaxed store is reported by TSan as a data race; disabling the reorder buffer gives 609 out-of-order chunks plus corrupted output |
| No lost wake-ups | Blocking tests in both directions; stress repeated 10x in CI, half pinned to one CPU |
| Pool | Exactly-once delivery, recursive fan-out, `-EDEADLK` detection, drain/cancel accounting, shutdown racing 4 submitters |
| Pipeline | Ordered output byte-identical across 5 shapes and both queue kinds; peak in-flight ≤ limit; worker, sink and reader error injection; cancel from another thread; empty input |
| Workloads | Word count against a sequential reference, including chunk sizes that put words on chunk boundaries; upper-casing against sequential output at chunk sizes 1, 7, 4 KiB and 1 MiB |
| End to end | CLI against `tr`, a Python `Counter` and `zlib.crc32`; SIGINT with idle input exits 130 |
| Tooling | TSan, ASan+UBSan and valgrind on every suite; gcc and clang; clang-tidy |

**Bugs the tooling found during development:**
- **UBSan:** two `memcpy` calls with NULL pointers and zero length.
- **TSan:** an intermittent end-to-end failure. It showed that an
  asynchronous signal handler on whichever thread the kernel picked was
  the wrong design. The CLI now blocks SIGINT/SIGTERM in all threads and
  receives them in a dedicated `sigwait` thread.

## 7. Benchmarks

Dev sandbox, 2 vCPUs (`fpbench`). Thread counts above 2 oversubscribe the
CPUs.

| queue shape | lock-free Mops/s | mutex Mops/s | ratio |
|---|---|---|---|
| 1 × 1 | 14.96 | 4.02 | 3.72x |
| 2 × 2 | 12.27 | 5.68 | 2.16x |
| 4 × 4 | 8.97 | 4.58 | 1.96x |
| 4 × 1 | 5.71 | 4.07 | 1.40x |
| 1 × 4 | 5.06 | 3.59 | 1.41x |

**Thread pool.** With empty tasks (pure overhead), the two queues are
level at 1 and 2 workers. At 4 workers, mutex wins (1.87M vs 1.09M
tasks/s): that is the wake-all thundering herd. With ~200-iteration tasks,
lock-free wins at 1 and 2 workers (1.8M vs 1.0M, 1.96M vs 1.58M) and still
loses at 4 workers on 2 CPUs. `fp_pool_config.queue_kind` lets callers
choose.

**Pipeline** (128 MiB in-memory corpus, 1 MiB chunks):

| workers | word count | upper-case |
|---|---|---|
| 1 | 168 MiB/s | 1,343 MiB/s |
| 2 | 365 MiB/s | 1,961 MiB/s |
| 4 | 313 MiB/s | 1,540 MiB/s |

Word count is CPU-bound and scales with cores. Upper-casing is close to
memory-bandwidth-bound. Four workers on two CPUs only adds scheduling
overhead.

## 8. Limitations and future work

- **Wake-one eventcount** to remove the herd on idle pools. This needs
  per-waiter wake tokens, or a queue of waiters.
- **Work stealing.** The pool uses one shared queue. Per-worker deques
  with stealing would cut contention on the single `deq_pos`.
- **Blocking reads.** A cancelled pipeline waits at most 100 ms on a pipe,
  but a read that blocks on a slow regular-file device is not interruptible.
- **Long tokens.** Tokens longer than a whole chunk are split across
  chunks.
