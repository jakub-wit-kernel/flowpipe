// SPDX-License-Identifier: MIT
/*
 * fpbench - micro and macro benchmarks; prints Markdown tables.
 *
 *   fpbench [--quick] [queue|pool|pipeline]...
 *
 *   queue     lock-free vs mutex queue, P producers / C consumers, ops/s
 *   pool      tiny-task throughput (submit + run an empty task)
 *   pipeline  word count and upper-case throughput vs worker count
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "flowpipe/pool.h"
#include "flowpipe/queue.h"
#include "flowpipe/wordcount.h"

static int quick;

static double now_s(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ---- queue ------------------------------------------------------------- */

struct qarg {
	fp_queue *q;
	long n;
	int *producers_left;
};

static void *qprod(void *arg)
{
	struct qarg *a = arg;

	for (long i = 0; i < a->n; i++) {
		uint64_t v = (uint64_t)i;

		fp_queue_push(a->q, &v);
	}
	if (__atomic_sub_fetch(a->producers_left, 1, __ATOMIC_ACQ_REL) == 0)
		fp_queue_close(a->q);
	return NULL;
}

static void *qcons(void *arg)
{
	struct qarg *a = arg;
	uint64_t v;
	volatile uint64_t sink = 0;

	while (fp_queue_pop(a->q, &v) == 0)
		sink = sink + v;
	return NULL;
}

static double bench_queue(enum fp_queue_kind kind, int p, int c, long total)
{
	fp_queue *q;
	pthread_t tp[16], tc[16];
	struct qarg a;
	int left = p;
	double t0;

	fp_queue_create(&q, kind, 1024, sizeof(uint64_t));
	a = (struct qarg){ q, total / p, &left };
	t0 = now_s();
	for (int i = 0; i < c; i++)
		pthread_create(&tc[i], NULL, qcons, &a);
	for (int i = 0; i < p; i++)
		pthread_create(&tp[i], NULL, qprod, &a);
	for (int i = 0; i < p; i++)
		pthread_join(tp[i], NULL);
	for (int i = 0; i < c; i++)
		pthread_join(tc[i], NULL);
	double secs = now_s() - t0;

	fp_queue_destroy(q);
	long done = (total / p) * p;	/* integer split across producers */

	return (double)done / secs;
}

static void run_queue(void)
{
	static const int shapes[][2] = { { 1, 1 }, { 2, 2 }, { 4, 4 }, { 4, 1 }, { 1, 4 } };
	long total = quick ? 400000 : 4000000;

	printf("\n### Queue: %ld transfers of 8 bytes, capacity 1024\n\n", total);
	printf("| producers x consumers | lock-free Mops/s | mutex Mops/s | ratio |\n");
	printf("|---|---|---|---|\n");
	for (size_t i = 0; i < sizeof(shapes) / sizeof(shapes[0]); i++) {
		double lf = bench_queue(FP_QUEUE_LOCKFREE, shapes[i][0], shapes[i][1], total);
		double mx = bench_queue(FP_QUEUE_MUTEX, shapes[i][0], shapes[i][1], total);

		printf("| %d x %d | %.2f | %.2f | %.2fx |\n", shapes[i][0],
		       shapes[i][1], lf / 1e6, mx / 1e6, lf / mx);
	}
}

/* ---- pool -------------------------------------------------------------- */

static void noop(void *arg)
{
	(void)arg;
}

static void run_pool(void)
{
	static const int threads[] = { 1, 2, 4 };
	long n = quick ? 200000 : 2000000;

	printf("\n### Thread pool: %ld empty tasks submitted from one thread\n\n", n);
	printf("| workers | lock-free queue tasks/s | mutex queue tasks/s |\n");
	printf("|---|---|---|\n");
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) {
		double r[2];

		for (int k = 0; k < 2; k++) {
			struct fp_pool_config c = { threads[i], 4096,
						    k ? FP_QUEUE_MUTEX : FP_QUEUE_LOCKFREE };
			fp_pool *p;
			double t0;

			fp_pool_create(&p, &c);
			t0 = now_s();
			for (long j = 0; j < n; j++)
				fp_pool_submit(p, noop, NULL);
			fp_pool_wait_idle(p);
			r[k] = (double)n / (now_s() - t0);
			fp_pool_destroy(p);
		}
		printf("| %d | %.0f | %.0f |\n", threads[i], r[0], r[1]);
	}
}

/* ---- pipeline ---------------------------------------------------------- */

static int make_corpus_fd(size_t bytes)
{
	static const char *const words[] = {
		"the", "of", "and", "to", "in", "kernel", "thread", "queue",
		"memory", "lock", "free", "pipeline", "data", "fast", "slow",
		"epoll", "atomic", "barrier", "cache", "line",
	};
	int fd = memfd_create("corpus", MFD_CLOEXEC);
	char buf[1 << 16];
	size_t written = 0;
	uint32_t x = 7;

	while (written < bytes) {
		size_t len = 0;

		while (len < sizeof(buf) - 32) {
			x = x * 1103515245u + 12345u;
			const char *w = words[(x >> 8) % 20];
			size_t n = strlen(w);

			memcpy(buf + len, w, n); /* NOLINT(bugprone-not-null-terminated-result): word stream, not a C string */
			len += n;
			buf[len++] = (x >> 4) % 13 ? ' ' : '\n';
		}
		if (write(fd, buf, len) != (ssize_t)len)
			exit(1);
		written += len;
	}
	return fd;
}

static void run_pipeline(void)
{
	long cpus = sysconf(_SC_NPROCESSORS_ONLN);
	size_t bytes = quick ? (16u << 20) : (128u << 20);
	int fd = make_corpus_fd(bytes);
	off_t size = lseek(fd, 0, SEEK_END);
	int out = open("/dev/null", O_WRONLY | O_CLOEXEC);

	printf("\n### Pipeline: %.0f MiB corpus in memory (memfd), 1 MiB chunks, "
	       "%ld online CPUs\n\n", (double)size / 1048576.0, cpus);
	printf("| workers | wc MiB/s | upper MiB/s |\n|---|---|---|\n");
	for (int w = 1; w <= (cpus < 8 ? (int)cpus * 2 : 8); w *= 2) {
		struct fp_pipeline_config cfg;
		struct fp_wordcount_result res;
		double t0, wc, up;

		fp_pipeline_config_init(&cfg);
		cfg.workers = w;
		cfg.max_inflight = 4 * w;
		lseek(fd, 0, SEEK_SET);
		t0 = now_s();
		if (fp_wordcount(fd, &cfg, 10, &res, NULL))
			exit(1);
		wc = (double)size / 1048576.0 / (now_s() - t0);
		fp_wordcount_free(&res);

		lseek(fd, 0, SEEK_SET);
		t0 = now_s();
		if (fp_upper(fd, out, &cfg, NULL))
			exit(1);
		up = (double)size / 1048576.0 / (now_s() - t0);
		printf("| %d | %.0f | %.0f |\n", w, wc, up);
	}
	close(out);
	close(fd);
}

int main(int argc, char **argv)
{
	int any = 0;

	for (int i = 1; i < argc; i++)
		if (!strcmp(argv[i], "--quick"))
			quick = 1;
	printf("fpbench: %ld online CPUs%s\n", sysconf(_SC_NPROCESSORS_ONLN),
	       quick ? " (quick)" : "");
	for (int i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "queue"))
			run_queue(), any = 1;
		else if (!strcmp(argv[i], "pool"))
			run_pool(), any = 1;
		else if (!strcmp(argv[i], "pipeline"))
			run_pipeline(), any = 1;
	}
	if (!any) {
		run_queue();
		run_pool();
		run_pipeline();
	}
	return 0;
}
