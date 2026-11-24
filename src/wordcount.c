// SPDX-License-Identifier: MIT
#define _GNU_SOURCE
#include "flowpipe/wordcount.h"

#include <errno.h>
#include <poll.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "flowpipe/arena.h"
#include "flowpipe/pool.h"

/* ---- chunk reader that respects token boundaries --------------------- */

enum split_rule { SPLIT_WORD, SPLIT_LINE };

struct split_reader {
	int fd;
	enum split_rule rule;
	volatile int *cancel;	/* from the pipeline config, may be NULL */
	int may_block;		/* -1 unknown, 0 regular file, 1 pipe/tty/socket */
	unsigned char *carry;	/* tail of the previous read, not yet emitted */
	size_t carry_len;
	size_t carry_cap;
	bool eof;
};

static bool is_boundary(const struct split_reader *sr, unsigned char ch)
{
	return sr->rule == SPLIT_LINE ? ch == '\n' : !fp_is_word_byte(ch);
}

/*
 * read() that notices cancellation while waiting on a pipe, FIFO or
 * terminal, where a read can block indefinitely. Regular files never
 * block for long, so they skip the poll.
 */
static ssize_t cancellable_read(struct split_reader *sr, void *buf, size_t n)
{
	if (sr->may_block < 0) {
		struct stat st;

		sr->may_block = fstat(sr->fd, &st) == 0 && !S_ISREG(st.st_mode);
	}
	while (sr->may_block && sr->cancel) {
		struct pollfd p = { .fd = sr->fd, .events = POLLIN };

		if (__atomic_load_n(sr->cancel, __ATOMIC_RELAXED))
			return -ECANCELED;
		int r = poll(&p, 1, 100);

		if (r > 0)
			break;
		if (r < 0 && errno != EINTR)
			return -errno;
	}
	ssize_t got = read(sr->fd, buf, n);

	return got < 0 ? -errno : got;
}

/*
 * Fill c->in with: leftover tail from last time + fresh bytes, then cut
 * after the last boundary byte and keep the remainder for next time. If
 * the buffer holds no boundary at all (a token longer than the chunk), the
 * whole buffer is emitted and the token is split; documented limitation.
 */
static ssize_t split_read(void *ctx, fp_chunk *c)
{
	struct split_reader *sr = ctx;
	size_t len = sr->carry_len, cut;

	if (len)
		memcpy(c->in, sr->carry, len);
	sr->carry_len = 0;
	while (!sr->eof && len < c->in_cap) {
		ssize_t n = cancellable_read(sr, c->in + len, c->in_cap - len);

		if (n < 0) {
			if (n == -EINTR)
				continue;
			return n;
		}
		if (n == 0)
			sr->eof = true;
		len += (size_t)n;
	}
	if (sr->eof || len == 0)
		return (ssize_t)len;

	for (cut = len; cut > 0 && !is_boundary(sr, c->in[cut - 1]); cut--)
		;
	if (cut == 0 || cut == len)
		return (ssize_t)len;	/* no boundary, or nothing to carry */
	if (len - cut > sr->carry_cap) {
		unsigned char *p = realloc(sr->carry, len - cut);

		if (!p)
			return -ENOMEM;
		sr->carry = p;
		sr->carry_cap = len - cut;
	}
	memcpy(sr->carry, c->in + cut, len - cut);
	sr->carry_len = len - cut;
	return (ssize_t)cut;
}

/* ---- per-worker hash table (open addressing, linear probing) --------- */

struct entry {
	uint64_t hash;		/* 0 means empty */
	uint64_t count;
	const char *key;
	uint32_t len;
};

struct table {
	struct entry *e;
	size_t cap;		/* power of two */
	size_t n;
	fp_arena keys;
	uint64_t total;
	int err;
};

static uint64_t hash_bytes(const unsigned char *p, size_t n)
{
	uint64_t h = 1469598103934665603ull;	/* FNV-1a 64 */

	while (n--)
		h = (h ^ *p++) * 1099511628211ull;
	return h ? h : 1;
}

static int table_init(struct table *t, size_t cap)
{
	memset(t, 0, sizeof(*t));
	t->e = calloc(cap, sizeof(*t->e));
	t->cap = cap;
	fp_arena_init(&t->keys, (size_t)256 * 1024);
	return t->e ? 0 : -ENOMEM;
}

static void table_free(struct table *t)
{
	free(t->e);
	fp_arena_free(&t->keys);
	memset(t, 0, sizeof(*t));
}

static int table_grow(struct table *t)
{
	size_t ncap = t->cap * 2;
	struct entry *ne = calloc(ncap, sizeof(*ne));

	if (!ne)
		return -ENOMEM;
	for (size_t i = 0; i < t->cap; i++) {
		if (!t->e[i].hash)
			continue;
		size_t j = t->e[i].hash & (ncap - 1);

		while (ne[j].hash)
			j = (j + 1) & (ncap - 1);
		ne[j] = t->e[i];
	}
	free(t->e);
	t->e = ne;
	t->cap = ncap;
	return 0;
}

/* Add @count occurrences of a word (already lower-cased). */
static int table_add(struct table *t, const char *w, size_t len, uint64_t h,
		     uint64_t count)
{
	size_t i;

	if ((t->n + 1) * 10 > t->cap * 7 && table_grow(t))
		return -ENOMEM;		/* keep load factor <= 0.7 */
	for (i = h & (t->cap - 1); t->e[i].hash; i = (i + 1) & (t->cap - 1)) {
		if (t->e[i].hash == h && t->e[i].len == len &&
		    memcmp(t->e[i].key, w, len) == 0) {
			t->e[i].count += count;
			return 0;
		}
	}
	const char *key = fp_arena_strndup(&t->keys, w, len);

	if (!key)
		return -ENOMEM;
	t->e[i] = (struct entry){ h, count, key, (uint32_t)len };
	t->n++;
	return 0;
}

/* ---- word count ----------------------------------------------------------- */

struct wc_ctx {
	struct split_reader sr;
	struct table *tables;	/* one per worker: no sharing on the hot path */
};

static int wc_work(void *ctx, int worker, fp_chunk *c)
{
	struct wc_ctx *wc = ctx;
	struct table *t = &wc->tables[worker];
	char word[256];
	size_t i = 0, n = c->in_len;

	while (i < n) {
		while (i < n && !fp_is_word_byte(c->in[i]))
			i++;
		size_t start = i, len = 0;

		while (i < n && fp_is_word_byte(c->in[i]))
			i++;
		if (i == start)
			break;
		len = i - start;
		/* Fold case into a scratch copy; long tokens use the arena. */
		char *w = len <= sizeof(word) ? word :
			  fp_arena_alloc(&t->keys, len, 1);

		if (!w)
			return -ENOMEM;
		for (size_t k = 0; k < len; k++) {
			unsigned char ch = c->in[start + k];

			w[k] = (char)(ch >= 'A' && ch <= 'Z' ? ch + 32 : ch);
		}
		if (table_add(t, w, len, hash_bytes((unsigned char *)w, len), 1))
			return -ENOMEM;
		t->total++;
	}
	return 0;
}

/* Parallel merge: worker tables are split into hash partitions; one pool
 * task merges each partition, so no two tasks touch the same key. */
struct merge_task {
	struct table *src;
	int nsrc;
	int part, nparts;
	struct table out;
	int err;
};

static void merge_partition(void *arg)
{
	struct merge_task *m = arg;

	if ((m->err = table_init(&m->out, 1024)))
		return;
	for (int s = 0; s < m->nsrc; s++) {
		const struct table *t = &m->src[s];

		for (size_t i = 0; i < t->cap; i++) {
			const struct entry *e = &t->e[i];

			/* High hash bits pick the partition; the tables index
			 * with the low bits, so the two stay independent. */
			if (!e->hash ||
			    (int)((e->hash >> 32) % (uint64_t)m->nparts) != m->part)
				continue;
			if ((m->err = table_add(&m->out, e->key, e->len, e->hash,
						e->count)))
				return;
		}
	}
}

static int word_cmp(const void *a, const void *b)
{
	const struct fp_word *x = a, *y = b;

	if (x->count != y->count)
		return x->count > y->count ? -1 : 1;
	return strcmp(x->word, y->word);
}

/* Is @a ranked strictly worse than @b (lower count, or same count and
 * lexicographically later)? */
static bool worse(const struct fp_word *a, const struct fp_word *b)
{
	return word_cmp(a, b) > 0;
}

/* Bounded min-heap ordered by rank: heap[0] is the worst of the kept set. */
static void heap_offer(struct fp_word *heap, size_t *n, size_t k,
		       struct fp_word w)
{
	size_t i;

	if (*n < k) {
		i = (*n)++;
		heap[i] = w;
		while (i && worse(&heap[i], &heap[(i - 1) / 2])) {
			struct fp_word t = heap[i];

			heap[i] = heap[(i - 1) / 2];
			heap[(i - 1) / 2] = t;
			i = (i - 1) / 2;
		}
		return;
	}
	if (!worse(&heap[0], &w))
		return;		/* not better than the current worst */
	heap[0] = w;
	for (i = 0;;) {
		size_t l = 2 * i + 1, r = l + 1, m = i;

		if (l < *n && worse(&heap[l], &heap[m]))
			m = l;
		if (r < *n && worse(&heap[r], &heap[m]))
			m = r;
		if (m == i)
			break;
		struct fp_word t = heap[i];

		heap[i] = heap[m];
		heap[m] = t;
		i = m;
	}
}

struct wc_priv {
	struct merge_task *parts;
	int nparts;
};

int fp_wordcount(int fd, const struct fp_pipeline_config *cfg_in,
		 size_t top_k, struct fp_wordcount_result *res,
		 struct fp_pipeline_stats *stats)
{
	struct fp_pipeline_config cfg;
	struct fp_pipeline_ops ops = { .read = split_read, .work = wc_work };
	struct wc_ctx wc = { .sr = { .fd = fd, .rule = SPLIT_WORD,
				     .may_block = -1 } };
	struct wc_priv *priv = NULL;
	fp_pool *pool = NULL;
	int ret, nparts;

	memset(res, 0, sizeof(*res));
	fp_pipeline_config_init(&cfg);
	if (cfg_in)
		cfg = *cfg_in;
	cfg.ordered = false;	/* counting is order-independent */
	wc.sr.cancel = cfg.cancel;

	wc.tables = calloc((size_t)cfg.workers, sizeof(*wc.tables));
	if (!wc.tables)
		return -ENOMEM;
	for (int i = 0; i < cfg.workers; i++)
		if ((ret = table_init(&wc.tables[i], 4096)))
			goto out;

	ret = fp_pipeline_run(&cfg, &ops, &wc, stats);
	if (ret)
		goto out;

	/* Merge per-worker tables in parallel on a thread pool. */
	nparts = cfg.workers > 1 ? cfg.workers * 2 : 1;
	priv = calloc(1, sizeof(*priv));
	if (!priv || !(priv->parts = calloc((size_t)nparts, sizeof(*priv->parts)))) {
		ret = -ENOMEM;
		goto out;
	}
	priv->nparts = nparts;
	struct fp_pool_config pc;

	fp_pool_config_init(&pc);
	pc.threads = cfg.workers;
	if ((ret = fp_pool_create(&pool, &pc)))
		goto out;
	for (int p = 0; p < nparts; p++) {
		priv->parts[p] = (struct merge_task){ wc.tables, cfg.workers, p,
						      nparts, { 0 }, 0 };
		fp_pool_submit(pool, merge_partition, &priv->parts[p]);
	}
	fp_pool_wait_idle(pool);

	size_t kept = 0;

	res->top = calloc(top_k ? top_k : 1, sizeof(*res->top));
	if (!res->top) {
		ret = -ENOMEM;
		goto out;
	}
	for (int i = 0; i < cfg.workers; i++)
		res->total_words += wc.tables[i].total;
	for (int p = 0; p < nparts; p++) {
		struct table *t = &priv->parts[p].out;

		if (priv->parts[p].err) {
			ret = priv->parts[p].err;
			goto out;
		}
		res->distinct_words += t->n;
		for (size_t i = 0; i < t->cap; i++)
			if (t->e[i].hash && top_k)
				heap_offer(res->top, &kept, top_k,
					   (struct fp_word){ t->e[i].key,
							     t->e[i].count });
	}
	qsort(res->top, kept, sizeof(*res->top), word_cmp);
	res->top_n = kept;
	res->priv = priv;
	priv = NULL;		/* keys live in the partition arenas */
out:
	fp_pool_destroy(pool);
	for (int i = 0; i < cfg.workers; i++)
		table_free(&wc.tables[i]);
	free(wc.tables);
	free(wc.sr.carry);
	if (priv) {
		for (int p = 0; priv->parts && p < priv->nparts; p++)
			table_free(&priv->parts[p].out);
		free(priv->parts);
		free(priv);
	}
	if (ret) {
		free(res->top);
		memset(res, 0, sizeof(*res));
	}
	return ret;
}

void fp_wordcount_free(struct fp_wordcount_result *res)
{
	struct wc_priv *priv = res->priv;

	if (priv) {
		for (int p = 0; p < priv->nparts; p++)
			table_free(&priv->parts[p].out);
		free(priv->parts);
		free(priv);
	}
	free(res->top);
	memset(res, 0, sizeof(*res));
}

/* ---- ordered transform ----------------------------------------------------- */

struct up_ctx {
	struct split_reader sr;
	int out_fd;
};

static int up_work(void *ctx, int worker, fp_chunk *c)
{
	(void)ctx;
	(void)worker;
	if (fp_chunk_reserve_out(c, c->in_len))
		return -ENOMEM;
	for (size_t i = 0; i < c->in_len; i++) {
		unsigned char ch = c->in[i];

		c->out[i] = ch >= 'a' && ch <= 'z' ? (unsigned char)(ch - 32) : ch;
	}
	c->out_len = c->in_len;
	return 0;
}

static int up_write(void *ctx, const fp_chunk *c)
{
	struct up_ctx *u = ctx;
	size_t off = 0;

	while (off < c->out_len) {
		ssize_t n = write(u->out_fd, c->out + off, c->out_len - off);

		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		off += (size_t)n;
	}
	return 0;
}

int fp_upper(int in_fd, int out_fd, const struct fp_pipeline_config *cfg_in,
	     struct fp_pipeline_stats *stats)
{
	struct fp_pipeline_config cfg;
	struct fp_pipeline_ops ops = { split_read, up_work, up_write };
	struct up_ctx u = { .sr = { .fd = in_fd, .rule = SPLIT_LINE,
				    .may_block = -1 },
			    .out_fd = out_fd };
	int ret;

	fp_pipeline_config_init(&cfg);
	if (cfg_in)
		cfg = *cfg_in;
	cfg.ordered = true;
	u.sr.cancel = cfg.cancel;
	ret = fp_pipeline_run(&cfg, &ops, &u, stats);
	free(u.sr.carry);
	return ret;
}
