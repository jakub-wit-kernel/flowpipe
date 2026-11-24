// SPDX-License-Identifier: MIT
/*
 * Queue tests, run against both implementations.
 *
 * The stress test checks the properties a correct MPMC FIFO must have:
 *   - every pushed element is popped exactly once (no loss, no duplicate)
 *   - for any one consumer, elements from any one producer arrive in the
 *     order that producer pushed them (per-producer FIFO)
 */
#define _GNU_SOURCE
#define KT_IMPLEMENTATION
#include "kt.h"

#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include "flowpipe/queue.h"

static const enum fp_queue_kind kinds[] = { FP_QUEUE_LOCKFREE, FP_QUEUE_MUTEX };

static long iters(long dflt)
{
	const char *e = getenv("FP_STRESS_SCALE");

	return e ? dflt * strtol(e, NULL, 10) : dflt;
}

KT_TEST(fifo_full_empty_single_thread)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_queue *q;
		uint64_t v;

		KT_EQ(fp_queue_create(&q, kinds[k], 8, sizeof(v)), 0);
		KT_EQ(fp_queue_capacity(q), 8);
		KT_EQ(fp_queue_try_pop(q, &v), -EAGAIN);
		for (v = 0; v < 8; v++)
			KT_EQ(fp_queue_try_push(q, &v), 0);
		KT_EQ(fp_queue_size(q), 8);
		KT_EQ(fp_queue_try_push(q, &v), -EAGAIN);
		for (uint64_t want = 0; want < 8; want++) {
			KT_EQ(fp_queue_try_pop(q, &v), 0);
			KT_EQ(v, want);
		}
		KT_EQ(fp_queue_try_pop(q, &v), -EAGAIN);
		fp_queue_destroy(q);
	}
}

KT_TEST(wraps_around_many_laps)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_queue *q;
		uint32_t in = 0, out = 0, v;

		KT_EQ(fp_queue_create(&q, kinds[k], 4, sizeof(v)), 0);
		for (int lap = 0; lap < 10000; lap++) {
			for (int i = 0; i < 3; i++, in++)
				KT_EQ(fp_queue_push(q, &in), 0);
			for (int i = 0; i < 3; i++, out++) {
				KT_EQ(fp_queue_pop(q, &v), 0);
				KT_EQ(v, out);
			}
		}
		fp_queue_destroy(q);
	}
}

KT_TEST(capacity_rounds_up_for_lockfree)
{
	fp_queue *q;

	KT_EQ(fp_queue_create(&q, FP_QUEUE_LOCKFREE, 100, 4), 0);
	KT_EQ(fp_queue_capacity(q), 128);
	fp_queue_destroy(q);
	KT_EQ(fp_queue_create(&q, FP_QUEUE_LOCKFREE, 1, 4), -EINVAL);
	KT_EQ(fp_queue_create(&q, FP_QUEUE_MUTEX, 16, 0), -EINVAL);
}

KT_TEST(large_elements_round_trip)
{
	struct big {
		char bytes[200];
	} a, b;

	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_queue *q;

		KT_EQ(fp_queue_create(&q, kinds[k], 4, sizeof(a)), 0);
		for (int i = 0; i < 100; i++) {
			memset(a.bytes, i, sizeof(a.bytes));
			KT_EQ(fp_queue_push(q, &a), 0);
			KT_EQ(fp_queue_pop(q, &b), 0);
			KT_ASSERT(memcmp(&a, &b, sizeof(a)) == 0);
		}
		fp_queue_destroy(q);
	}
}

KT_TEST(close_drains_then_fails)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_queue *q;
		int v = 7;

		KT_EQ(fp_queue_create(&q, kinds[k], 4, sizeof(v)), 0);
		KT_EQ(fp_queue_push(q, &v), 0);
		fp_queue_close(q);
		KT_EQ(fp_queue_push(q, &v), -EPIPE);
		KT_EQ(fp_queue_try_push(q, &v), -EPIPE);
		v = 0;
		KT_EQ(fp_queue_pop(q, &v), 0);		/* still drains */
		KT_EQ(v, 7);
		KT_EQ(fp_queue_pop(q, &v), -EPIPE);
		KT_EQ(fp_queue_try_pop(q, &v), -EPIPE);
		fp_queue_destroy(q);
	}
}

/* ---- blocking behaviour ---------------------------------------------- */

struct blk {
	fp_queue *q;
	int result;
	volatile int done;
};

static void *blocked_pop(void *arg)
{
	struct blk *b = arg;
	int v;

	b->result = fp_queue_pop(b->q, &v);
	__atomic_store_n(&b->done, 1, __ATOMIC_RELEASE);
	return NULL;
}

static void *blocked_push(void *arg)
{
	struct blk *b = arg;
	int v = 1;

	b->result = fp_queue_push(b->q, &v);
	__atomic_store_n(&b->done, 1, __ATOMIC_RELEASE);
	return NULL;
}

static void sleep_ms(int ms)
{
	struct timespec ts = { 0, (long)ms * 1000000L };

	nanosleep(&ts, NULL);
}

KT_TEST(close_wakes_blocked_consumers_and_producers)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_queue *empty, *full;
		struct blk c[3], p[3];
		pthread_t tc[3], tp[3];
		int v = 0;

		KT_EQ(fp_queue_create(&empty, kinds[k], 2, sizeof(int)), 0);
		KT_EQ(fp_queue_create(&full, kinds[k], 2, sizeof(int)), 0);
		while (fp_queue_try_push(full, &v) == 0)
			;
		for (int i = 0; i < 3; i++) {
			c[i] = (struct blk){ empty, 99, 0 };
			p[i] = (struct blk){ full, 99, 0 };
			pthread_create(&tc[i], NULL, blocked_pop, &c[i]);
			pthread_create(&tp[i], NULL, blocked_push, &p[i]);
		}
		sleep_ms(100);
		for (int i = 0; i < 3; i++)
			KT_ASSERT(!__atomic_load_n(&c[i].done, __ATOMIC_ACQUIRE) &&
				  !__atomic_load_n(&p[i].done, __ATOMIC_ACQUIRE));
		fp_queue_close(empty);
		fp_queue_close(full);
		for (int i = 0; i < 3; i++) {
			pthread_join(tc[i], NULL);
			pthread_join(tp[i], NULL);
			KT_EQ(c[i].result, -EPIPE);
			KT_EQ(p[i].result, -EPIPE);
		}
		fp_queue_destroy(empty);
		fp_queue_destroy(full);
	}
}

KT_TEST(blocked_producer_wakes_when_space_frees)
{
	for (size_t k = 0; k < KT_N(kinds); k++) {
		fp_queue *q;
		struct blk p;
		pthread_t t;
		int v = 0;

		KT_EQ(fp_queue_create(&q, kinds[k], 2, sizeof(int)), 0);
		while (fp_queue_try_push(q, &v) == 0)
			;
		p = (struct blk){ q, 99, 0 };
		pthread_create(&t, NULL, blocked_push, &p);
		sleep_ms(50);
		KT_ASSERT(!__atomic_load_n(&p.done, __ATOMIC_ACQUIRE));
		KT_EQ(fp_queue_pop(q, &v), 0);
		pthread_join(t, NULL);
		KT_EQ(p.result, 0);
		fp_queue_destroy(q);
	}
}

/* ---- MPMC stress ----------------------------------------------------- */

#define MAXP 8
#define MAXC 8

struct stress {
	fp_queue *q;
	int nprod, ncons;
	long per_prod;
	uint8_t *seen;			/* [nprod * per_prod], atomic increments */
	int remaining_producers;	/* atomic; last one closes the queue */
	char err[160];
	int failed;			/* atomic */
};

struct worker {
	struct stress *s;
	int id;
};

static void stress_fail(struct stress *s, const char *msg)
{
	if (__atomic_fetch_add(&s->failed, 1, __ATOMIC_RELAXED) == 0)
		snprintf(s->err, sizeof(s->err), "%s", msg);
}

static void *producer(void *arg)
{
	struct worker *w = arg;
	struct stress *s = w->s;

	for (long i = 0; i < s->per_prod; i++) {
		uint64_t v = ((uint64_t)w->id << 40) | (uint64_t)i;

		if (fp_queue_push(s->q, &v) != 0) {
			stress_fail(s, "push failed before close");
			break;
		}
	}
	/* Last producer out closes: no push can race with the close. */
	if (__atomic_sub_fetch(&s->remaining_producers, 1, __ATOMIC_ACQ_REL) == 0)
		fp_queue_close(s->q);
	return NULL;
}

static void *consumer(void *arg)
{
	struct worker *w = arg;
	struct stress *s = w->s;
	long last[MAXP];
	uint64_t v;

	for (int i = 0; i < MAXP; i++)
		last[i] = -1;
	while (fp_queue_pop(s->q, &v) == 0) {
		int p = (int)(v >> 40);
		long i = (long)(v & ((1ull << 40) - 1));

		if (p >= s->nprod || i >= s->per_prod) {
			stress_fail(s, "value out of range");
			break;
		}
		if (i <= last[p]) {
			stress_fail(s, "per-producer order violated");
			break;
		}
		last[p] = i;
		__atomic_fetch_add(&s->seen[(size_t)p * (size_t)s->per_prod + (size_t)i],
				   1, __ATOMIC_RELAXED);
	}
	return NULL;
}

/*
 * A broken queue usually loses or duplicates elements, and lost elements
 * leave consumers blocked forever. Turn that hang into a clear failure.
 */
static const char *watchdog_what = "";

static void watchdog(int sig)
{
	static const char msg[] =
		"not ok - stress run made no progress for 120 s "
		"(lost elements or deadlock): ";

	(void)sig;
	(void)!write(STDOUT_FILENO, msg, sizeof(msg) - 1);
	(void)!write(STDOUT_FILENO, watchdog_what, strlen(watchdog_what));
	(void)!write(STDOUT_FILENO, "\n", 1);
	_exit(1);
}

static void run_stress(enum fp_queue_kind kind, int nprod, int ncons,
		       size_t cap, long per_prod)
{
	static char what[96];
	struct stress s = { .nprod = nprod, .ncons = ncons,
			    .per_prod = per_prod,
			    .remaining_producers = nprod };
	struct worker wp[MAXP], wc[MAXC];
	pthread_t tp[MAXP], tc[MAXC];

	snprintf(what, sizeof(what), "%s %dP/%dC cap %zu",
		 fp_queue_kind_name(kind), nprod, ncons, cap);
	watchdog_what = what;
	signal(SIGALRM, watchdog);
	alarm(120);
	KT_EQ(fp_queue_create(&s.q, kind, cap, sizeof(uint64_t)), 0);
	s.seen = calloc((size_t)nprod * (size_t)per_prod, 1);
	KT_ASSERT(s.seen);
	for (int i = 0; i < ncons; i++) {
		wc[i] = (struct worker){ &s, i };
		pthread_create(&tc[i], NULL, consumer, &wc[i]);
	}
	for (int i = 0; i < nprod; i++) {
		wp[i] = (struct worker){ &s, i };
		pthread_create(&tp[i], NULL, producer, &wp[i]);
	}
	for (int i = 0; i < nprod; i++)
		pthread_join(tp[i], NULL);
	for (int i = 0; i < ncons; i++)
		pthread_join(tc[i], NULL);
	alarm(0);

	if (s.failed)
		KT_FAIL("%s %dP/%dC cap %zu: %s", fp_queue_kind_name(kind),
			nprod, ncons, cap, s.err);
	for (size_t i = 0; i < (size_t)nprod * (size_t)per_prod; i++)
		if (s.seen[i] != 1)
			KT_FAIL("%s %dP/%dC: producer %zu item %zu seen %u times",
				fp_queue_kind_name(kind), nprod, ncons,
				i / (size_t)per_prod, i % (size_t)per_prod,
				s.seen[i]);
	free(s.seen);
	fp_queue_destroy(s.q);
}

KT_TEST(stress_exactly_once_and_per_producer_order)
{
	static const struct { int p, c; size_t cap; } shapes[] = {
		{ 1, 1, 2 }, { 1, 1, 1024 }, { 4, 1, 4 }, { 1, 4, 4 },
		{ 4, 4, 2 }, { 4, 4, 64 }, { 8, 8, 16 },
	};

	for (size_t k = 0; k < KT_N(kinds); k++)
		for (size_t i = 0; i < KT_N(shapes); i++)
			run_stress(kinds[k], shapes[i].p, shapes[i].c,
				   shapes[i].cap, iters(50000));
}

static const struct kt_case cases[] = {
	KT_CASE(fifo_full_empty_single_thread),
	KT_CASE(wraps_around_many_laps),
	KT_CASE(capacity_rounds_up_for_lockfree),
	KT_CASE(large_elements_round_trip),
	KT_CASE(close_drains_then_fails),
	KT_CASE(close_wakes_blocked_consumers_and_producers),
	KT_CASE(blocked_producer_wakes_when_space_frees),
	KT_CASE(stress_exactly_once_and_per_producer_order),
};

int main(int argc, char **argv)
{
	return kt_run(cases, KT_N(cases), argc, argv);
}
