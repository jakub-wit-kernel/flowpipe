/* SPDX-License-Identifier: MIT */
/*
 * Workloads built on fp_pipeline:
 *
 *   fp_wordcount   word frequencies (unordered pipeline). Each worker counts
 *                  into its own hash table, so the hot path shares nothing;
 *                  the tables are merged at the end.
 *   fp_upper       ASCII upper-casing (ordered pipeline): output is
 *                  byte-identical to a sequential run.
 *
 * A "word" is a maximal run of ASCII letters, digits, '_' or '\''; letters
 * are folded to lower case. The reader never splits a word (or, for
 * fp_upper, a line) across chunks unless it is longer than a whole chunk.
 */
#ifndef FLOWPIPE_WORDCOUNT_H
#define FLOWPIPE_WORDCOUNT_H

#include <stddef.h>
#include <stdint.h>

#include "flowpipe/pipeline.h"

#ifdef __cplusplus
extern "C" {
#endif

struct fp_word {
	const char *word;
	uint64_t count;
};

struct fp_wordcount_result {
	struct fp_word *top;	/* top_n words: count desc, then word asc */
	size_t top_n;
	uint64_t total_words;
	uint64_t distinct_words;
	void *priv;		/* owns the memory behind top[].word */
};

/* Count words read from @fd. 0 or -errno. Free with fp_wordcount_free. */
int fp_wordcount(int fd, const struct fp_pipeline_config *cfg, size_t top_k,
		 struct fp_wordcount_result *res,
		 struct fp_pipeline_stats *stats);
void fp_wordcount_free(struct fp_wordcount_result *res);

/* Copy @in_fd to @out_fd with ASCII letters upper-cased. 0 or -errno. */
int fp_upper(int in_fd, int out_fd, const struct fp_pipeline_config *cfg,
	     struct fp_pipeline_stats *stats);

/* Tokenizer shared with the tests' reference implementation. */
static inline int fp_is_word_byte(unsigned char ch)
{
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
	       (ch >= '0' && ch <= '9') || ch == '_' || ch == '\'';
}

#ifdef __cplusplus
}
#endif

#endif /* FLOWPIPE_WORDCOUNT_H */
