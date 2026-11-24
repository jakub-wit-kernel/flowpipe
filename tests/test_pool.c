// SPDX-License-Identifier: MIT
#define _GNU_SOURCE
#define KT_IMPLEMENTATION
#include "kt.h"

#include <pthread.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include "flowpipe/pool.h"

static const enum fp_queue_kind kinds[] = { FP_QUEUE_LOCKFREE, FP_QUEUE_MUTEX };

static fp_pool *make_pool(int threads, size_t cap, enum fp_queue_kind kind)
{
	struct fp_pool_config c = { threads, cap, kind };
	fp_pool *p;

	if (fp_pool_create(&p, &c))
		KT_FAIL("fp_pool_create");
	return p;
}

static void sleep_ms(int ms)
{
	struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

	nanosleep(&ts, NULL);
}

static uint64_t g_counter;

static void incr(void *arg)
{
	(void)arg;
	__atomic_fetch_add(&g_counter, 1, __ATOMIC_RELAXED);
}

KT_TEST(runs_every_task_and_waits_idle)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_pool *p = make_pool(4, 64, kinds[k]);
		struct fp_pool_stats st;
		uint64_t sum = 0;

		g_counter = 0;
		for (int i = 0; i < 100000; i++)
			KT_EQ(fp_pool_submit(p, incr, NULL), 0);
		KT_EQ(fp_pool_wait_idle(p), 0);
		KT_EQ(__atomic_load_n(&g_counter, __ATOMIC_RELAXED), 100000);
		fp_pool_stats(p, &st);
		KT_EQ(st.submitted, 100000);
		KT_EQ(st.completed, 100000);
		for (int i = 0; i < 4; i++)
			sum += fp_pool_worker_executed(p, i);
		KT_EQ(sum, 100000);
		fp_pool_destroy(p);
	}
}

/* Each task writes its own slot: verifies every argument is delivered once. */
static void mark(void *arg)
{
	__atomic_fetch_add((uint8_t *)arg, 1, __ATOMIC_RELAXED);
}

KT_TEST(each_argument_delivered_exactly_once)
{
	enum { N = 50000 };
	static uint8_t slots[N];
	fp_pool *p = make_pool(3, 8, FP_QUEUE_LOCKFREE);

	memset(slots, 0, sizeof(slots));
	for (int i = 0; i < N; i++)
		KT_EQ(fp_pool_submit(p, mark, &slots[i]), 0);
	fp_pool_wait_idle(p);
	for (int i = 0; i < N; i++)
		KT_EQ(slots[i], 1);
	fp_pool_destroy(p);
}

/* Tasks that submit more tasks: a recursive fan-out. */
struct fan {
	fp_pool *p;
	int depth;
};

static struct fan fan_nodes[1 << 12];
static int fan_next;

static void fan_out(void *arg)
{
	struct fan *f = arg;

	__atomic_fetch_add(&g_counter, 1, __ATOMIC_RELAXED);
	if (f->depth == 0)
		return;
	for (int i = 0; i < 2; i++) {
		int idx = __atomic_fetch_add(&fan_next, 1, __ATOMIC_RELAXED);

		fan_nodes[idx] = (struct fan){ f->p, f->depth - 1 };
		/* try_submit + inline fallback avoids blocking a worker. */
		if (fp_pool_try_submit(f->p, fan_out, &fan_nodes[idx]) != 0)
			fan_out(&fan_nodes[idx]);
	}
}

KT_TEST(tasks_can_submit_tasks)
{
	fp_pool *p = make_pool(4, 16, FP_QUEUE_LOCKFREE);
	struct fan root = { p, 10 };

	g_counter = 0;
	fan_next = 0;
	KT_EQ(fp_pool_submit(p, fan_out, &root), 0);
	KT_EQ(fp_pool_wait_idle(p), 0);
	KT_EQ(g_counter, (1 << 11) - 1);	/* full binary tree */
	fp_pool_destroy(p);
}

static int deadlk_result;

static void wait_from_worker(void *arg)
{
	deadlk_result = fp_pool_wait_idle(arg);
}

KT_TEST(wait_idle_from_worker_is_rejected)
{
	fp_pool *p = make_pool(2, 4, FP_QUEUE_LOCKFREE);

	KT_EQ(fp_pool_submit(p, wait_from_worker, p), 0);
	fp_pool_wait_idle(p);
	KT_EQ(deadlk_result, -EDEADLK);
	KT_EQ(fp_pool_current_worker(), -1);
	fp_pool_destroy(p);
}

static void slow(void *arg)
{
	(void)arg;
	sleep_ms(20);
	__atomic_fetch_add(&g_counter, 1, __ATOMIC_RELAXED);
}

KT_TEST(try_submit_reports_full_queue)
{
	fp_pool *p = make_pool(1, 2, FP_QUEUE_LOCKFREE);
	int accepted = 0, full = 0;

	g_counter = 0;
	for (int i = 0; i < 20; i++) {
		int r = fp_pool_try_submit(p, slow, NULL);

		accepted += r == 0;
		full += r == -EAGAIN;
	}
	KT_ASSERT(full > 0);
	KT_EQ(accepted + full, 20);
	fp_pool_wait_idle(p);
	KT_EQ(g_counter, (uint64_t)accepted);
	fp_pool_destroy(p);
}

KT_TEST(drain_shutdown_runs_queued_tasks)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_pool *p = make_pool(1, 64, kinds[k]);

		g_counter = 0;
		for (int i = 0; i < 20; i++)
			KT_EQ(fp_pool_submit(p, slow, NULL), 0);
		KT_EQ(fp_pool_shutdown(p, FP_SHUTDOWN_DRAIN, NULL), 0);
		KT_EQ(g_counter, 20);
		KT_EQ(fp_pool_submit(p, slow, NULL), -EPIPE);
		fp_pool_destroy(p);
	}
}

static uint64_t g_cancelled;

static void on_cancel(void *arg)
{
	(void)arg;
	__atomic_fetch_add(&g_cancelled, 1, __ATOMIC_RELAXED);
}

KT_TEST(cancel_shutdown_hands_back_queued_tasks)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_pool *p = make_pool(1, 64, kinds[k]);
		struct fp_pool_stats st;
		size_t n;

		g_counter = 0;
		g_cancelled = 0;
		for (int i = 0; i < 30; i++)
			KT_EQ(fp_pool_submit(p, slow, NULL), 0);
		sleep_ms(30);	/* let one or two start */
		n = fp_pool_shutdown(p, FP_SHUTDOWN_CANCEL, on_cancel);
		fp_pool_stats(p, &st);
		/* Every task either ran or was cancelled; none was lost. */
		KT_EQ(g_counter + g_cancelled, 30);
		KT_EQ(n, g_cancelled);
		KT_ASSERT(g_cancelled > 0 && g_counter > 0);
		KT_EQ(st.completed + st.cancelled, 30);
		KT_EQ(fp_pool_shutdown(p, FP_SHUTDOWN_DRAIN, NULL), n); /* idempotent */
		fp_pool_destroy(p);
	}
}

/* Submitters racing with shutdown: every submit either fails with -EPIPE
 * or its task runs. Nothing is silently dropped. */
struct racer {
	fp_pool *p;
	int ok, rejected;
};

static void *submit_racer(void *arg)
{
	struct racer *r = arg;

	for (int i = 0; i < 20000; i++) {
		int ret = fp_pool_submit(r->p, incr, NULL);

		if (ret == 0)
			r->ok++;
		else if (ret == -EPIPE)
			r->rejected++;
	}
	return NULL;
}

KT_TEST(shutdown_races_with_submitters_safely)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_pool *p = make_pool(2, 16, kinds[k]);
		struct racer r[4];
		pthread_t t[4];
		int ok = 0, rejected = 0;

		g_counter = 0;
		for (int i = 0; i < 4; i++) {
			r[i] = (struct racer){ p, 0, 0 };
			pthread_create(&t[i], NULL, submit_racer, &r[i]);
		}
		sleep_ms(5);
		fp_pool_shutdown(p, FP_SHUTDOWN_DRAIN, NULL);
		for (int i = 0; i < 4; i++) {
			pthread_join(t[i], NULL);
			ok += r[i].ok;
			rejected += r[i].rejected;
		}
		KT_EQ(ok + rejected, 4 * 20000);
		KT_EQ(g_counter, (uint64_t)ok);
		fp_pool_destroy(p);
	}
}

static const struct kt_case cases[] = {
	KT_CASE(runs_every_task_and_waits_idle),
	KT_CASE(each_argument_delivered_exactly_once),
	KT_CASE(tasks_can_submit_tasks),
	KT_CASE(wait_idle_from_worker_is_rejected),
	KT_CASE(try_submit_reports_full_queue),
	KT_CASE(drain_shutdown_runs_queued_tasks),
	KT_CASE(cancel_shutdown_hands_back_queued_tasks),
	KT_CASE(shutdown_races_with_submitters_safely),
};

int main(int argc, char **argv)
{
	return kt_run(cases, KT_N(cases), argc, argv);
}
