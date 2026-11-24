/* SPDX-License-Identifier: MIT */
/*
 * fp_pipeline - bounded, parallel, optionally order-preserving pipeline:
 *
 *   reader thread ──► work queue ──► N worker threads ──► done queue ──► sink
 *        ▲                                                                │
 *        └──────────────────────── free queue ◄───────────────────────────┘
 *
 * A fixed set of max_inflight chunks circulates through the stages, so
 * memory is bounded no matter how fast the reader is (backpressure: the
 * reader blocks on the free queue). With ordered = true the sink emits
 * chunks in input order using a reorder buffer of max_inflight slots.
 *
 * The first error (or cancellation) from any stage stops the pipeline:
 * the reader stops producing, workers skip remaining work, everything is
 * joined, and fp_pipeline_run() returns that error.
 */
#ifndef FLOWPIPE_PIPELINE_H
#define FLOWPIPE_PIPELINE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include "flowpipe/queue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fp_chunk {
	uint64_t seq;		/* input order, from 0 */
	unsigned char *in;	/* in_cap bytes, filled by read */
	size_t in_len;
	size_t in_cap;
	unsigned char *out;	/* output, grown with fp_chunk_reserve_out */
	size_t out_len;
	size_t out_cap;
} fp_chunk;

/* Ensure chunk->out can hold @n bytes. 0 or -ENOMEM. */
int fp_chunk_reserve_out(fp_chunk *c, size_t n);

struct fp_pipeline_ops {
	/* Reader thread, sequential. Fill c->in; return bytes, 0 at end, or -errno. */
	ssize_t (*read)(void *ctx, fp_chunk *c);
	/* Worker threads, concurrent. worker is in [0, workers). 0 or -errno. */
	int (*work)(void *ctx, int worker, fp_chunk *c);
	/* Sink (calling thread), sequential. Optional. 0 or -errno. */
	int (*write)(void *ctx, const fp_chunk *c);
};

struct fp_pipeline_config {
	int workers;			/* default: online CPUs */
	size_t chunk_size;		/* default 1 MiB */
	int max_inflight;		/* default 4 * workers */
	bool ordered;			/* default true */
	enum fp_queue_kind queue_kind;	/* default FP_QUEUE_LOCKFREE */
	/*
	 * Optional: set non-zero to stop (from any thread with an atomic store,
	 * or from a signal handler). Checked by the reader between chunks.
	 */
	volatile int *cancel;
};

struct fp_pipeline_stats {
	uint64_t chunks;
	uint64_t bytes_in;
	uint64_t bytes_out;
	int max_inflight_seen;	/* chunks between read and release, peak */
	int max_reorder_depth;	/* chunks parked waiting for an earlier seq */
};

void fp_pipeline_config_init(struct fp_pipeline_config *cfg);
/* Runs to completion. Returns 0, the first stage error, or -ECANCELED. */
int fp_pipeline_run(const struct fp_pipeline_config *cfg,
		    const struct fp_pipeline_ops *ops, void *ctx,
		    struct fp_pipeline_stats *stats);

#ifdef __cplusplus
}
#endif

#endif /* FLOWPIPE_PIPELINE_H */
