/* SPDX-License-Identifier: MIT */
/*
 * fp_pool - fixed-size thread pool over a bounded task queue.
 *
 *   - Submission blocks when the queue is full (backpressure); try_submit
 *     returns -EAGAIN instead.
 *   - fp_pool_wait_idle() waits until every task submitted so far has
 *     finished (tasks may submit further tasks).
 *   - Shutdown either drains (runs every queued task) or cancels (queued
 *     tasks are handed to a cancel callback so their arguments can be
 *     freed). Running tasks always finish; there is no forced kill.
 */
#ifndef FLOWPIPE_POOL_H
#define FLOWPIPE_POOL_H

#include <stddef.h>
#include <stdint.h>

#include "flowpipe/queue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fp_pool fp_pool;
typedef void (*fp_task_fn)(void *arg);

struct fp_pool_config {
	int threads;			/* default: online CPUs */
	size_t queue_capacity;		/* default 1024 */
	enum fp_queue_kind queue_kind;	/* default FP_QUEUE_LOCKFREE */
};

enum fp_shutdown_mode {
	FP_SHUTDOWN_DRAIN,	/* run everything already queued */
	FP_SHUTDOWN_CANCEL,	/* pass queued tasks to on_cancel instead */
};

struct fp_pool_stats {
	uint64_t submitted;
	uint64_t completed;
	uint64_t cancelled;
};

void fp_pool_config_init(struct fp_pool_config *cfg);
int fp_pool_create(fp_pool **out, const struct fp_pool_config *cfg);

/* 0, or -EPIPE once shutdown has begun. Blocks while the queue is full. */
int fp_pool_submit(fp_pool *p, fp_task_fn fn, void *arg);
/* 0, -EAGAIN if the queue is full, or -EPIPE. */
int fp_pool_try_submit(fp_pool *p, fp_task_fn fn, void *arg);

/*
 * Wait until no task is queued or running. Returns 0, or -EDEADLK when
 * called from one of this pool's own workers (it would wait for itself).
 */
int fp_pool_wait_idle(fp_pool *p);

/*
 * Stop accepting work, then drain or cancel the queue and join all
 * workers. @on_cancel may be NULL. Returns the number of tasks cancelled.
 * Idempotent; must not be called from a worker of this pool.
 */
size_t fp_pool_shutdown(fp_pool *p, enum fp_shutdown_mode mode,
			fp_task_fn on_cancel);
/* Shuts down (drain) if still running, then frees. */
void fp_pool_destroy(fp_pool *p);

int fp_pool_threads(const fp_pool *p);
void fp_pool_stats(const fp_pool *p, struct fp_pool_stats *out);
/* Tasks executed by worker @i (for load-balance checks). */
uint64_t fp_pool_worker_executed(const fp_pool *p, int i);
/* Index of the calling worker thread in its pool, or -1. */
int fp_pool_current_worker(void);

#ifdef __cplusplus
}
#endif

#endif /* FLOWPIPE_POOL_H */
