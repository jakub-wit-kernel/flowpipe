// SPDX-License-Identifier: MIT
/*
 * flowpipe - parallel data processing from the command line.
 *
 *   flowpipe wc    [opts] [FILE]          top word frequencies
 *   flowpipe upper [opts] [FILE] [-o OUT]  ASCII upper-case, order preserved
 *   flowpipe sum   [-j N] FILE...         CRC-32 of many files in parallel
 *
 * Options: -j/--jobs N  -c/--chunk-kib N  --inflight N  --top K
 *          --queue lockfree|mutex  --stats
 * FILE "-" or omitted means stdin. Ctrl-C cancels cleanly.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "flowpipe/pool.h"
#include "flowpipe/wordcount.h"

static volatile int g_cancel;

/*
 * SIGINT/SIGTERM are blocked in every thread and received synchronously
 * here. That avoids depending on which thread the kernel picks to run an
 * asynchronous handler (or on when a sanitizer runtime chooses to run it).
 */
static void *signal_thread(void *arg)
{
	sigset_t *set = arg;
	int sig;

	if (sigwait(set, &sig) == 0)
		__atomic_store_n(&g_cancel, 1, __ATOMIC_RELAXED);
	return NULL;
}

static long parse_long(const char *opt, const char *s, long min, long max)
{
	char *end;
	long v;

	errno = 0;
	v = strtol(s, &end, 10);
	if (errno || end == s || *end || v < min || v > max) {
		fprintf(stderr, "flowpipe: %s: expected %ld..%ld, got '%s'\n",
			opt, min, max, s);
		exit(2);
	}
	return v;
}

static double now_s(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void print_stats(const struct fp_pipeline_stats *st, double secs)
{
	fprintf(stderr,
		"flowpipe: %llu chunks, %.1f MiB in, %.1f MiB out, %.3f s "
		"(%.0f MiB/s), peak in-flight %d, peak reorder depth %d\n",
		(unsigned long long)st->chunks, (double)st->bytes_in / 1048576.0,
		(double)st->bytes_out / 1048576.0, secs,
		(double)st->bytes_in / 1048576.0 / (secs > 0 ? secs : 1e-9),
		st->max_inflight_seen, st->max_reorder_depth);
}

/* ---- sum: one pool task per file -------------------------------------- */

static uint32_t crc_table[256];

static void crc_init(void)
{
	for (uint32_t i = 0; i < 256; i++) {
		uint32_t c = i;

		for (int k = 0; k < 8; k++)
			c = c & 1 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
		crc_table[i] = c;
	}
}

struct sum_job {
	const char *path;
	uint32_t crc;
	uint64_t size;
	int err;
};

static void sum_file(void *arg)
{
	struct sum_job *j = arg;
	unsigned char buf[1 << 16];
	uint32_t crc = 0xFFFFFFFFu;
	int fd = open(j->path, O_RDONLY | O_CLOEXEC);
	ssize_t n;

	if (fd < 0) {
		j->err = errno;
		return;
	}
	while ((n = read(fd, buf, sizeof(buf))) > 0) {
		for (ssize_t i = 0; i < n; i++)
			crc = crc_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
		j->size += (uint64_t)n;
	}
	if (n < 0)
		j->err = errno;
	close(fd);
	j->crc = crc ^ 0xFFFFFFFFu;
}

static int cmd_sum(int jobs, int argc, char **argv)
{
	struct fp_pool_config pc;
	struct sum_job *js;
	fp_pool *pool;
	int status = 0, r;

	if (argc == 0) {
		fprintf(stderr, "flowpipe sum: no files\n");
		return 2;
	}
	crc_init();
	fp_pool_config_init(&pc);
	if (jobs)
		pc.threads = jobs;
	js = calloc((size_t)argc, sizeof(*js));
	if (!js)
		return 1;
	r = fp_pool_create(&pool, &pc);
	if (r) {
		fprintf(stderr, "flowpipe sum: cannot start pool: %s\n",
			strerror(-r));
		free(js);
		return 1;
	}
	for (int i = 0; i < argc; i++) {
		js[i].path = argv[i];
		fp_pool_submit(pool, sum_file, &js[i]);
	}
	fp_pool_wait_idle(pool);
	fp_pool_destroy(pool);
	/* Results print in argument order regardless of completion order. */
	for (int i = 0; i < argc; i++) {
		if (js[i].err) {
			fprintf(stderr, "flowpipe sum: %s: %s\n", js[i].path,
				strerror(js[i].err));
			status = 1;
			continue;
		}
		printf("%08x %llu %s\n", js[i].crc,
		       (unsigned long long)js[i].size, js[i].path);
	}
	free(js);
	return status;
}

/* ---- main ---------------------------------------------------------------- */

static void usage(void)
{
	fprintf(stderr,
		"usage: flowpipe wc    [-j N] [-c KiB] [--inflight N] [--top K] [--queue lockfree|mutex] [--stats] [FILE]\n"
		"       flowpipe upper [-j N] [-c KiB] [--inflight N] [--queue lockfree|mutex] [--stats] [FILE] [-o OUT]\n"
		"       flowpipe sum   [-j N] FILE...\n");
	exit(2);
}

int main(int argc, char **argv)
{
	static const struct option lo[] = {
		{ "jobs", required_argument, NULL, 'j' },
		{ "chunk-kib", required_argument, NULL, 'c' },
		{ "inflight", required_argument, NULL, 'i' },
		{ "top", required_argument, NULL, 't' },
		{ "queue", required_argument, NULL, 'q' },
		{ "output", required_argument, NULL, 'o' },
		{ "stats", no_argument, NULL, 's' },
		{ "help", no_argument, NULL, 'h' },
		{ 0 }
	};
	struct fp_pipeline_config cfg;
	struct fp_pipeline_stats st;
	const char *cmd, *out_path = NULL;
	int opt, jobs = 0, show_stats = 0, in_fd = 0, out_fd = 1, ret;
	long top = 20;
	double t0;

	if (argc < 2)
		usage();
	cmd = argv[1];
	fp_pipeline_config_init(&cfg);
	optind = 2;
	while ((opt = getopt_long(argc, argv, "j:c:o:h", lo, NULL)) != -1) {
		switch (opt) {
		case 'j': jobs = (int)parse_long("--jobs", optarg, 1, 1024); break;
		case 'c': cfg.chunk_size = (size_t)parse_long("--chunk-kib", optarg, 1, 1 << 20) * 1024; break;
		case 'i': cfg.max_inflight = (int)parse_long("--inflight", optarg, 1, 1 << 16); break;
		case 't': top = parse_long("--top", optarg, 0, 1 << 20); break;
		case 'q':
			if (strcmp(optarg, "mutex") == 0)
				cfg.queue_kind = FP_QUEUE_MUTEX;
			else if (strcmp(optarg, "lockfree") != 0)
				usage();
			break;
		case 'o': out_path = optarg; break;
		case 's': show_stats = 1; break;
		default: usage();
		}
	}
	if (jobs) {
		/* Keep the default 4 chunks per worker unless set explicitly. */
		if (cfg.max_inflight == 4 * cfg.workers)
			cfg.max_inflight = 4 * jobs;
		cfg.workers = jobs;
	}
	if (strcmp(cmd, "sum") == 0)
		return cmd_sum(jobs, argc - optind, argv + optind);
	if (strcmp(cmd, "wc") != 0 && strcmp(cmd, "upper") != 0)
		usage();
	if (argc - optind > 1)
		usage();
	if (optind < argc && strcmp(argv[optind], "-") != 0) {
		in_fd = open(argv[optind], O_RDONLY | O_CLOEXEC);
		if (in_fd < 0) {
			fprintf(stderr, "flowpipe: %s: %s\n", argv[optind],
				strerror(errno));
			return 1;
		}
	}

	static sigset_t sigs;
	pthread_t sigthr;

	sigemptyset(&sigs);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &sigs, NULL);	/* inherited by all threads */
	if (pthread_create(&sigthr, NULL, signal_thread, &sigs) == 0)
		pthread_detach(sigthr);
	cfg.cancel = &g_cancel;
	t0 = now_s();

	if (strcmp(cmd, "wc") == 0) {
		struct fp_wordcount_result res;

		ret = fp_wordcount(in_fd, &cfg, (size_t)top, &res, &st);
		if (ret == 0) {
			for (size_t i = 0; i < res.top_n; i++)
				printf("%llu %s\n",
				       (unsigned long long)res.top[i].count,
				       res.top[i].word);
			fprintf(stderr, "flowpipe: %llu words, %llu distinct\n",
				(unsigned long long)res.total_words,
				(unsigned long long)res.distinct_words);
			fp_wordcount_free(&res);
		}
	} else {
		if (out_path) {
			out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC |
						O_CLOEXEC, 0644);
			if (out_fd < 0) {
				fprintf(stderr, "flowpipe: %s: %s\n", out_path,
					strerror(errno));
				return 1;
			}
		}
		ret = fp_upper(in_fd, out_fd, &cfg, &st);
		if (out_fd != 1 && close(out_fd) && !ret)
			ret = -errno;
	}
	if (show_stats)
		print_stats(&st, now_s() - t0);
	if (ret == -ECANCELED) {
		fprintf(stderr, "flowpipe: cancelled\n");
		return 130;
	}
	if (ret) {
		fprintf(stderr, "flowpipe: %s\n", strerror(-ret));
		return 1;
	}
	return 0;
}
