// SPDX-License-Identifier: MIT
/*
 * Pipeline tests with a synthetic in-memory source. Workers sleep for
 * pseudo-random times so chunks finish out of order, which exercises the
 * reorder buffer.
 */
#define _GNU_SOURCE
#define KT_IMPLEMENTATION
#include "kt.h"

#include <pthread.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include "flowpipe/pipeline.h"

struct src {
	const unsigned char *data;
	size_t len, off;
	size_t read_size;		/* bytes per read() (<= chunk) */
	uint64_t fail_work_at;		/* inject a work error on this seq+1 */
	uint64_t fail_write_at;		/* inject a write error on this seq+1 */
	int max_sleep_us;
	unsigned char *out;		/* sink appends here */
	size_t out_len;
	uint64_t last_seq_written;
	int out_of_order;
	int writes;
};

static ssize_t src_read(void *ctx, fp_chunk *c)
{
	struct src *s = ctx;
	size_t n = s->len - s->off;

	if (n > s->read_size)
		n = s->read_size;
	if (n > c->in_cap)
		n = c->in_cap;
	memcpy(c->in, s->data + s->off, n);
	s->off += n;
	return (ssize_t)n;
}

static int xform_work(void *ctx, int worker, fp_chunk *c)
{
	struct src *s = ctx;

	(void)worker;
	if (s->fail_work_at && c->seq + 1 == s->fail_work_at)
		return -EIO;
	if (s->max_sleep_us) {
		/* Deterministic per-chunk jitter: later chunks often finish first. */
		long us = (long)((c->seq * 2654435761u) % (uint64_t)s->max_sleep_us);
		struct timespec ts = { 0, us * 1000L };

		nanosleep(&ts, NULL);
	}
	if (fp_chunk_reserve_out(c, c->in_len))
		return -ENOMEM;
	for (size_t i = 0; i < c->in_len; i++)
		c->out[i] = (unsigned char)(c->in[i] ^ 0x5A);
	c->out_len = c->in_len;
	return 0;
}

static int sink_write(void *ctx, const fp_chunk *c)
{
	struct src *s = ctx;

	if (s->fail_write_at && c->seq + 1 == s->fail_write_at)
		return -ENOSPC;
	if (s->writes++ && c->seq != s->last_seq_written + 1)
		s->out_of_order++;
	s->last_seq_written = c->seq;
	memcpy(s->out + s->out_len, c->out, c->out_len);
	s->out_len += c->out_len;
	return 0;
}

static const struct fp_pipeline_ops ops = { src_read, xform_work, sink_write };

static unsigned char *make_data(size_t n)
{
	unsigned char *d = malloc(n);

	for (size_t i = 0; i < n; i++)
		d[i] = (unsigned char)(i * 131 + (i >> 11));
	return d;
}

static void cfg_for(struct fp_pipeline_config *cfg, int workers, int inflight,
		    size_t chunk, bool ordered, enum fp_queue_kind kind)
{
	fp_pipeline_config_init(cfg);
	cfg->workers = workers;
	cfg->max_inflight = inflight;
	cfg->chunk_size = chunk;
	cfg->ordered = ordered;
	cfg->queue_kind = kind;
}

KT_TEST(ordered_output_matches_sequential)
{
	static const enum fp_queue_kind kinds[] = { FP_QUEUE_LOCKFREE, FP_QUEUE_MUTEX };
	static const struct { int w, inflight; size_t chunk; } shapes[] = {
		{ 1, 1, 4096 }, { 4, 4, 4096 }, { 4, 16, 1000 }, { 8, 3, 777 },
		{ 3, 64, 65536 },
	};
	const size_t n = 3u << 20;
	unsigned char *data = make_data(n);

	for (size_t k = 0; k < KT_N(kinds); k++) {
		for (size_t i = 0; i < KT_N(shapes); i++) {
			struct src s = { .data = data, .len = n,
					 .read_size = shapes[i].chunk,
					 .max_sleep_us = 300 };
			struct fp_pipeline_config cfg;
			struct fp_pipeline_stats st;

			s.out = malloc(n);
			cfg_for(&cfg, shapes[i].w, shapes[i].inflight,
				shapes[i].chunk, true, kinds[k]);
			KT_EQ(fp_pipeline_run(&cfg, &ops, &s, &st), 0);
			KT_EQ(s.out_len, n);
			KT_EQ(s.out_of_order, 0);
			for (size_t b = 0; b < n; b++)
				if (s.out[b] != (data[b] ^ 0x5A))
					KT_FAIL("byte %zu wrong (w=%d inflight=%d)",
						b, shapes[i].w, shapes[i].inflight);
			KT_EQ(st.bytes_in, n);
			KT_EQ(st.bytes_out, n);
			/* Memory bound: never more chunks alive than allowed. */
			KT_ASSERT(st.max_inflight_seen <= shapes[i].inflight);
			KT_ASSERT(st.max_reorder_depth <= shapes[i].inflight);
			free(s.out);
		}
	}
	free(data);
}

KT_TEST(unordered_mode_delivers_everything)
{
	const size_t n = 1u << 20;
	unsigned char *data = make_data(n);
	struct src s = { .data = data, .len = n, .read_size = 1000,
			 .max_sleep_us = 200 };
	struct fp_pipeline_config cfg;
	struct fp_pipeline_stats st;
	uint64_t sum_in = 0, sum_out = 0;

	s.out = malloc(n);
	cfg_for(&cfg, 4, 8, 1000, false, FP_QUEUE_LOCKFREE);
	KT_EQ(fp_pipeline_run(&cfg, &ops, &s, &st), 0);
	KT_EQ(s.out_len, n);
	/* Order may differ; the multiset of bytes must not. */
	for (size_t i = 0; i < n; i++) {
		sum_in += (uint64_t)(data[i] ^ 0x5A) * 2654435761u;
		sum_out += (uint64_t)s.out[i] * 2654435761u;
	}
	KT_EQ(sum_in, sum_out);
	KT_EQ(st.chunks, (n + 999) / 1000);
	free(s.out);
	free(data);
}

KT_TEST(empty_input)
{
	struct src s = { .data = (const unsigned char *)"", .len = 0,
			 .read_size = 16 };
	struct fp_pipeline_config cfg;
	struct fp_pipeline_stats st;

	cfg_for(&cfg, 2, 4, 16, true, FP_QUEUE_LOCKFREE);
	KT_EQ(fp_pipeline_run(&cfg, &ops, &s, &st), 0);
	KT_EQ(st.chunks, 0);
	KT_EQ(s.writes, 0);
}

KT_TEST(worker_error_stops_pipeline_and_is_returned)
{
	const size_t n = 4u << 20;
	unsigned char *data = make_data(n);

	for (int ordered = 0; ordered < 2; ordered++) {
		struct src s = { .data = data, .len = n, .read_size = 4096,
				 .fail_work_at = 50, .max_sleep_us = 50 };
		struct fp_pipeline_config cfg;
		struct fp_pipeline_stats st;

		s.out = malloc(n);
		cfg_for(&cfg, 4, 8, 4096, ordered, FP_QUEUE_LOCKFREE);
		KT_EQ(fp_pipeline_run(&cfg, &ops, &s, &st), -EIO);
		/* It stopped early rather than reading the whole input. */
		KT_ASSERT(s.off < n);
		if (ordered)	/* nothing at or after the failed chunk is written */
			KT_ASSERT(s.writes <= 49);
		free(s.out);
	}
	free(data);
}

KT_TEST(sink_error_stops_pipeline)
{
	const size_t n = 4u << 20;
	unsigned char *data = make_data(n);
	struct src s = { .data = data, .len = n, .read_size = 4096,
			 .fail_write_at = 10 };
	struct fp_pipeline_config cfg;

	s.out = malloc(n);
	cfg_for(&cfg, 4, 8, 4096, true, FP_QUEUE_MUTEX);
	KT_EQ(fp_pipeline_run(&cfg, &ops, &s, NULL), -ENOSPC);
	KT_EQ(s.writes, 9);
	KT_ASSERT(s.off < n);
	free(s.out);
	free(data);
}

static ssize_t failing_read(void *ctx, fp_chunk *c)
{
	struct src *s = ctx;

	if (s->off >= 20 * 1024)
		return -EBADF;
	return src_read(ctx, c);
}

KT_TEST(reader_error_is_returned)
{
	const size_t n = 1u << 20;
	unsigned char *data = make_data(n);
	struct src s = { .data = data, .len = n, .read_size = 1024 };
	struct fp_pipeline_ops o = { failing_read, xform_work, sink_write };
	struct fp_pipeline_config cfg;

	s.out = malloc(n);
	cfg_for(&cfg, 2, 4, 1024, true, FP_QUEUE_LOCKFREE);
	KT_EQ(fp_pipeline_run(&cfg, &o, &s, NULL), -EBADF);
	free(s.out);
	free(data);
}

struct canceller {
	volatile int *flag;
	int after_ms;
};

static void *cancel_later(void *arg)
{
	struct canceller *c = arg;
	struct timespec ts = { 0, (long)c->after_ms * 1000000L };

	nanosleep(&ts, NULL);
	__atomic_store_n(c->flag, 1, __ATOMIC_RELAXED);
	return NULL;
}

KT_TEST(cancel_from_another_thread)
{
	const size_t n = 64u << 20;
	unsigned char *data = make_data(n);
	struct src s = { .data = data, .len = n, .read_size = 4096,
			 .max_sleep_us = 500 };
	volatile int flag = 0;
	struct canceller cn = { &flag, 50 };
	struct fp_pipeline_config cfg;
	pthread_t t;

	s.out = malloc(n);
	cfg_for(&cfg, 2, 4, 4096, true, FP_QUEUE_LOCKFREE);
	cfg.cancel = &flag;
	pthread_create(&t, NULL, cancel_later, &cn);
	KT_EQ(fp_pipeline_run(&cfg, &ops, &s, NULL), -ECANCELED);
	pthread_join(t, NULL);
	KT_ASSERT(s.off < n);
	free(s.out);
	free(data);
}

KT_TEST(invalid_config_rejected)
{
	struct fp_pipeline_config cfg;
	struct fp_pipeline_ops bad = { NULL, xform_work, NULL };

	fp_pipeline_config_init(&cfg);
	KT_EQ(fp_pipeline_run(&cfg, &bad, NULL, NULL), -EINVAL);
	cfg.workers = 0;
	KT_EQ(fp_pipeline_run(&cfg, &ops, NULL, NULL), -EINVAL);
}

static const struct kt_case cases[] = {
	KT_CASE(ordered_output_matches_sequential),
	KT_CASE(unordered_mode_delivers_everything),
	KT_CASE(empty_input),
	KT_CASE(worker_error_stops_pipeline_and_is_returned),
	KT_CASE(sink_error_stops_pipeline),
	KT_CASE(reader_error_is_returned),
	KT_CASE(cancel_from_another_thread),
	KT_CASE(invalid_config_rejected),
};

int main(int argc, char **argv)
{
	return kt_run(cases, KT_N(cases), argc, argv);
}
