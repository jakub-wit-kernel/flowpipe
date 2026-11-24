// SPDX-License-Identifier: MIT
/*
 * Word count and upper-case transform, checked against simple sequential
 * reference implementations, across worker counts and tiny chunk sizes
 * that force words to sit on chunk boundaries.
 */
#define _GNU_SOURCE
#define KT_IMPLEMENTATION
#include "kt.h"

#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>

#include "flowpipe/arena.h"
#include "flowpipe/wordcount.h"

/* ---- test corpus -------------------------------------------------------- */

static const char *const vocab[] = {
	"the", "The", "THE", "kernel", "Kernel", "epoll", "mutex", "atomic",
	"don't", "snake_case", "x86_64", "a", "of", "and", "queue", "fence",
	"lock-free", "data-race", "zipf", "alpha", "beta", "gamma", "pipeline",
};

/* Zipf-ish text with mixed punctuation, newlines and long tokens. */
static char *make_corpus(size_t target, size_t *len_out)
{
	char *buf = malloc(target + 512);
	size_t len = 0;
	uint32_t x = 12345;

	while (len < target) {
		x = x * 1103515245u + 12345u;
		unsigned r = (x >> 8) % 1000;
		size_t idx = r < 400 ? 0 : r < 600 ? (r % 3) : (r % KT_N(vocab));

		if (r == 999) {			/* occasional very long token */
			memset(buf + len, 'z', 300);
			len += 300;
		} else {
			size_t n = strlen(vocab[idx]);

			memcpy(buf + len, vocab[idx], n);
			len += n;
		}
		static const char seps[] = " , . ;\n!? \t";

		buf[len++] = seps[(x >> 20) % (sizeof(seps) - 1)];
	}
	*len_out = len;
	return buf;
}

static int memfd_with(const char *data, size_t len)
{
	int fd = memfd_create("corpus", MFD_CLOEXEC);

	if (fd < 0 || write(fd, data, len) != (ssize_t)len)
		return -1;
	lseek(fd, 0, SEEK_SET);
	return fd;
}

/* ---- reference word count (single-threaded, obviously correct) -------- */

struct ref_entry {
	char *w;
	uint64_t n;
};

static int ref_cmp(const void *a, const void *b)
{
	const struct ref_entry *x = a, *y = b;

	if (x->n != y->n)
		return x->n > y->n ? -1 : 1;
	return strcmp(x->w, y->w);
}

static struct ref_entry *reference_count(const char *d, size_t len,
					 size_t *distinct, uint64_t *total)
{
	struct ref_entry *e = NULL;
	size_t ne = 0, cap = 0, i = 0;

	*total = 0;
	while (i < len) {
		while (i < len && !fp_is_word_byte((unsigned char)d[i]))
			i++;
		size_t s = i;

		while (i < len && fp_is_word_byte((unsigned char)d[i]))
			i++;
		if (i == s)
			break;
		char *w = strndup(d + s, i - s);

		for (char *p = w; *p; p++)
			if (*p >= 'A' && *p <= 'Z')
				*p = (char)(*p + 32);
		(*total)++;
		size_t k;

		for (k = 0; k < ne && strcmp(e[k].w, w) != 0; k++)
			;
		if (k < ne) {
			e[k].n++;
			free(w);
			continue;
		}
		if (ne == cap) {
			cap = cap ? cap * 2 : 64;
			struct ref_entry *ne_ = realloc(e, cap * sizeof(*e));

			if (!ne_)
				abort();	/* test reference: no recovery */
			e = ne_;
		}
		e[ne++] = (struct ref_entry){ w, 1 };
	}
	if (ne)
		qsort(e, ne, sizeof(*e), ref_cmp);
	*distinct = ne;
	return e;
}

/* ---- tests ---------------------------------------------------------------- */

KT_TEST(wordcount_matches_reference)
{
	static const struct { int workers; size_t chunk; } shapes[] = {
		{ 1, 1 << 20 }, { 4, 4096 }, { 4, 37 }, { 8, 512 }, { 3, 301 },
	};
	size_t len, distinct;
	uint64_t total;
	char *corpus = make_corpus(2u << 20, &len);
	struct ref_entry *ref = reference_count(corpus, len, &distinct, &total);

	for (size_t s = 0; s < KT_N(shapes); s++) {
		struct fp_pipeline_config cfg;
		struct fp_wordcount_result res;
		int fd = memfd_with(corpus, len);

		KT_ASSERT(fd >= 0);
		fp_pipeline_config_init(&cfg);
		cfg.workers = shapes[s].workers;
		cfg.max_inflight = 2 * shapes[s].workers;
		cfg.chunk_size = shapes[s].chunk;
		KT_EQ(fp_wordcount(fd, &cfg, 50, &res, NULL), 0);
		close(fd);

		/*
		 * Tokens longer than a chunk are split by design. Only the
		 * shapes whose chunk exceeds the longest token (300) must
		 * match the reference exactly.
		 */
		if (shapes[s].chunk > 300) {
			KT_EQ(res.total_words, total);
			KT_EQ(res.distinct_words, distinct);
			KT_EQ(res.top_n, distinct < 50 ? distinct : 50);
			for (size_t i = 0; i < res.top_n; i++) {
				if (strcmp(res.top[i].word, ref[i].w) != 0 ||
				    res.top[i].count != ref[i].n)
					KT_FAIL("rank %zu: got %s=%llu want %s=%llu "
						"(workers %d, chunk %zu)", i,
						res.top[i].word,
						(unsigned long long)res.top[i].count,
						ref[i].w, (unsigned long long)ref[i].n,
						shapes[s].workers, shapes[s].chunk);
			}
		} else {
			/* Short words never cross a boundary: still exact. */
			KT_EQ(res.top[0].count, ref[0].n);
		}
		fp_wordcount_free(&res);
	}
	for (size_t i = 0; i < distinct; i++)
		free(ref[i].w);
	free(ref);
	free(corpus);
}

KT_TEST(wordcount_top_zero_and_empty_input)
{
	struct fp_wordcount_result res;
	int fd = memfd_with("", 0);

	KT_EQ(fp_wordcount(fd, NULL, 10, &res, NULL), 0);
	KT_EQ(res.total_words, 0);
	KT_EQ(res.top_n, 0);
	fp_wordcount_free(&res);
	close(fd);

	fd = memfd_with("a b a", 5);
	KT_EQ(fp_wordcount(fd, NULL, 0, &res, NULL), 0);
	KT_EQ(res.total_words, 3);
	KT_EQ(res.distinct_words, 2);
	KT_EQ(res.top_n, 0);
	fp_wordcount_free(&res);
	close(fd);
}

KT_TEST(upper_is_byte_identical_to_sequential)
{
	static const size_t chunks[] = { 1, 7, 4096, 1 << 20 };
	size_t len;
	char *corpus = make_corpus(1u << 20, &len);

	for (size_t s = 0; s < KT_N(chunks); s++) {
		struct fp_pipeline_config cfg;
		struct fp_pipeline_stats st;
		int in = memfd_with(corpus, len);
		int out = memfd_create("out", MFD_CLOEXEC);
		char *got = malloc(len);

		fp_pipeline_config_init(&cfg);
		cfg.workers = 4;
		cfg.max_inflight = 8;
		cfg.chunk_size = chunks[s];
		KT_EQ(fp_upper(in, out, &cfg, &st), 0);
		KT_EQ(lseek(out, 0, SEEK_END), (off_t)len);
		KT_EQ(pread(out, got, len, 0), (ssize_t)len);
		for (size_t i = 0; i < len; i++) {
			char want = corpus[i] >= 'a' && corpus[i] <= 'z' ?
				    (char)(corpus[i] - 32) : corpus[i];

			if (got[i] != want)
				KT_FAIL("byte %zu differs (chunk %zu)", i,
					chunks[s]);
		}
		free(got);
		close(in);
		close(out);
	}
	free(corpus);
}

KT_TEST(arena_alignment_and_reuse)
{
	fp_arena a;

	fp_arena_init(&a, 128);
	for (int i = 0; i < 1000; i++) {
		size_t align = (size_t)1 << (i % 5);
		unsigned char *p = fp_arena_alloc(&a, (size_t)(i % 50) + 1, align);

		KT_ASSERT(p);
		KT_ASSERT(((uintptr_t)p & (align - 1)) == 0);
		memset(p, 0xAB, (size_t)(i % 50) + 1);	/* ASan checks bounds */
	}
	char *big = fp_arena_alloc(&a, 10000, 8);	/* larger than a block */

	KT_ASSERT(big);
	memset(big, 1, 10000);
	KT_ASSERT(fp_arena_alloc(&a, 8, 3) == NULL);	/* bad alignment */
	char *s = fp_arena_strndup(&a, "hello world", 5);

	KT_ASSERT(strcmp(s, "hello") == 0);
	KT_ASSERT(a.used > 10000 && a.reserved >= a.used);
	fp_arena_free(&a);
	KT_EQ(a.used, 0);
	KT_ASSERT(fp_arena_alloc(&a, 16, 8));	/* usable after free */
	fp_arena_free(&a);
}

static const struct kt_case cases[] = {
	KT_CASE(wordcount_matches_reference),
	KT_CASE(wordcount_top_zero_and_empty_input),
	KT_CASE(upper_is_byte_identical_to_sequential),
	KT_CASE(arena_alignment_and_reuse),
};

int main(int argc, char **argv)
{
	return kt_run(cases, KT_N(cases), argc, argv);
}
