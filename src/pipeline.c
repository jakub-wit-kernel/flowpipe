// SPDX-License-Identifier: MIT
#define _GNU_SOURCE
#include "flowpipe/pipeline.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct run {
	const struct fp_pipeline_config *cfg;
	const struct fp_pipeline_ops *ops;
	void *ctx;
	fp_chunk *chunks;
	fp_queue *free_q, *work_q, *done_q;
	_Atomic int error;		/* first error, 0 if none */
	_Atomic int workers_left;	/* last worker out closes done_q */
	_Atomic int inflight;
	_Atomic int max_inflight;
	_Atomic uint64_t bytes_in;
};

struct worker_arg {
	struct run *r;
	int idx;
};

int fp_chunk_reserve_out(fp_chunk *c, size_t n)
{
	size_t cap = c->out_cap ? c->out_cap : 4096;
	unsigned char *p;

	if (n <= c->out_cap)
		return 0;
	while (cap < n)
		cap *= 2;
	p = realloc(c->out, cap);
	if (!p)
		return -ENOMEM;
	c->out = p;
	c->out_cap = cap;
	return 0;
}

/* Record the first error and unblock the reader so the pipeline unwinds. */
static void set_error(struct run *r, int err)
{
	int expected = 0;

	if (atomic_compare_exchange_strong(&r->error, &expected, err))
		fp_queue_close(r->free_q);	/* reader waits here */
}

static bool failed(struct run *r)
{
	return atomic_load_explicit(&r->error, memory_order_acquire) != 0;
}

static void track_inflight(struct run *r, int delta)
{
	int now = atomic_fetch_add(&r->inflight, delta) + delta;
	int max = atomic_load(&r->max_inflight);

	while (now > max &&
	       !atomic_compare_exchange_weak(&r->max_inflight, &max, now))
		;
}

static void *reader_main(void *arg)
{
	struct run *r = arg;
	uint64_t seq = 0;
	fp_chunk *c;

	while (!failed(r)) {
		if (r->cfg->cancel &&
		    __atomic_load_n(r->cfg->cancel, __ATOMIC_RELAXED)) {
			set_error(r, -ECANCELED);
			break;
		}
		if (fp_queue_pop(r->free_q, &c) != 0)
			break;			/* closed by an error */
		ssize_t n = r->ops->read(r->ctx, c);

		if (n <= 0) {
			if (n < 0)
				set_error(r, (int)n);
			break;
		}
		c->seq = seq++;
		c->in_len = (size_t)n;
		c->out_len = 0;
		atomic_fetch_add_explicit(&r->bytes_in, (uint64_t)n,
					  memory_order_relaxed);
		track_inflight(r, 1);
		/* Never blocks: work_q holds every chunk that exists. */
		fp_queue_push(r->work_q, &c);
	}
	fp_queue_close(r->work_q);
	return NULL;
}

static void *worker_main(void *arg)
{
	struct worker_arg *wa = arg;
	struct run *r = wa->r;
	fp_chunk *c;

	while (fp_queue_pop(r->work_q, &c) == 0) {
		/* After an error, keep draining so the sink can release chunks. */
		if (!failed(r)) {
			int ret = r->ops->work(r->ctx, wa->idx, c);

			if (ret)
				set_error(r, ret);
		}
		fp_queue_push(r->done_q, &c);
	}
	if (atomic_fetch_sub(&r->workers_left, 1) == 1)
		fp_queue_close(r->done_q);
	return NULL;
}

void fp_pipeline_config_init(struct fp_pipeline_config *cfg)
{
	long n = sysconf(_SC_NPROCESSORS_ONLN);

	memset(cfg, 0, sizeof(*cfg));
	cfg->workers = n > 0 ? (int)n : 1;
	cfg->chunk_size = (size_t)1 << 20;
	cfg->max_inflight = 4 * cfg->workers;
	cfg->ordered = true;
	cfg->queue_kind = FP_QUEUE_LOCKFREE;
}

static void emit(struct run *r, fp_chunk *c, struct fp_pipeline_stats *st)
{
	if (!failed(r)) {
		if (r->ops->write) {
			int ret = r->ops->write(r->ctx, c);

			if (ret)
				set_error(r, ret);
		}
		st->chunks++;
		st->bytes_out += c->out_len;
	}
	track_inflight(r, -1);
	/* Fails with -EPIPE after an error closed free_q; the chunk is
	 * owned by r->chunks either way. */
	fp_queue_push(r->free_q, &c);
}

int fp_pipeline_run(const struct fp_pipeline_config *cfg_in,
		    const struct fp_pipeline_ops *ops, void *ctx,
		    struct fp_pipeline_stats *stats_out)
{
	struct fp_pipeline_config cfg;
	struct fp_pipeline_stats st = { 0 };
	struct run r = { 0 };
	struct worker_arg *wa = NULL;
	pthread_t reader, *wt = NULL;
	fp_chunk **slots = NULL;
	sigset_t all, old;
	int ret = 0, started = 0, m, depth = 0;
	uint64_t next = 0;
	fp_chunk *c;

	fp_pipeline_config_init(&cfg);
	if (cfg_in)
		cfg = *cfg_in;
	if (!ops || !ops->read || !ops->work || cfg.workers < 1 ||
	    cfg.chunk_size == 0 || cfg.max_inflight < 1)
		return -EINVAL;
	m = cfg.max_inflight;
	r.cfg = &cfg;
	r.ops = ops;
	r.ctx = ctx;
	atomic_init(&r.workers_left, cfg.workers);

	/*
	 * Queues hold chunk pointers (sizeof(fp_chunk *) below is intended);
	 * capacity m means pushes never block.
	 */
	r.chunks = calloc((size_t)m, sizeof(*r.chunks));
	slots = calloc((size_t)m, sizeof(*slots)); /* NOLINT(bugprone-sizeof-expression): array of pointers */
	wt = calloc((size_t)cfg.workers, sizeof(*wt));
	wa = calloc((size_t)cfg.workers, sizeof(*wa));
	if (!r.chunks || !slots || !wt || !wa ||
	    fp_queue_create(&r.free_q, cfg.queue_kind, (size_t)m < 2 ? 2 : (size_t)m,
			    sizeof(fp_chunk *)) || /* NOLINT(bugprone-sizeof-expression) */
	    fp_queue_create(&r.work_q, cfg.queue_kind, (size_t)m < 2 ? 2 : (size_t)m,
			    sizeof(fp_chunk *)) || /* NOLINT(bugprone-sizeof-expression) */
	    fp_queue_create(&r.done_q, cfg.queue_kind, (size_t)m < 2 ? 2 : (size_t)m,
			    sizeof(fp_chunk *))) { /* NOLINT(bugprone-sizeof-expression) */
		ret = -ENOMEM;
		goto out;
	}
	for (int i = 0; i < m; i++) {
		r.chunks[i].in = malloc(cfg.chunk_size);
		r.chunks[i].in_cap = cfg.chunk_size;
		if (!r.chunks[i].in) {
			ret = -ENOMEM;
			goto out;
		}
		c = &r.chunks[i];
		fp_queue_push(r.free_q, &c);
	}

	sigfillset(&all);
	pthread_sigmask(SIG_BLOCK, &all, &old);
	for (; started < cfg.workers; started++) {
		wa[started] = (struct worker_arg){ &r, started };
		if (pthread_create(&wt[started], NULL, worker_main,
				   &wa[started]))
			break;
	}
	if (started < cfg.workers ||
	    pthread_create(&reader, NULL, reader_main, &r)) {
		pthread_sigmask(SIG_SETMASK, &old, NULL);
		/* Unwind: no reader; let the started workers exit. */
		atomic_store(&r.workers_left, started);
		fp_queue_close(r.work_q);
		for (int i = 0; i < started; i++)
			pthread_join(wt[i], NULL);
		ret = -EAGAIN;
		goto out;
	}
	pthread_sigmask(SIG_SETMASK, &old, NULL);

	/* Sink: runs here until every worker has finished. */
	while (fp_queue_pop(r.done_q, &c) == 0) {
		if (!cfg.ordered) {
			emit(&r, c, &st);
			continue;
		}
		/* At most m chunks are in flight, so seq % m never collides. */
		slots[c->seq % (uint64_t)m] = c;
		depth++;
		while (slots[next % (uint64_t)m] &&
		       slots[next % (uint64_t)m]->seq == next) {
			fp_chunk *e = slots[next % (uint64_t)m];

			slots[next % (uint64_t)m] = NULL;
			depth--;
			next++;
			emit(&r, e, &st);
		}
		if (depth > st.max_reorder_depth)
			st.max_reorder_depth = depth;
	}
	pthread_join(reader, NULL);
	for (int i = 0; i < cfg.workers; i++)
		pthread_join(wt[i], NULL);
	ret = atomic_load(&r.error);

out:
	st.bytes_in = atomic_load(&r.bytes_in);
	st.max_inflight_seen = atomic_load(&r.max_inflight);
	if (stats_out)
		*stats_out = st;
	for (int i = 0; r.chunks && i < m; i++) {
		free(r.chunks[i].in);
		free(r.chunks[i].out);
	}
	fp_queue_destroy(r.free_q);
	fp_queue_destroy(r.work_q);
	fp_queue_destroy(r.done_q);
	free(r.chunks);
	free(slots);
	free(wt);
	free(wa);
	return ret;
}
