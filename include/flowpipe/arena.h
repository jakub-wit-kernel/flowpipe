/* SPDX-License-Identifier: MIT */
/*
 * fp_arena - bump allocator. Many small allocations with one lifetime
 * (e.g. every word key in a per-thread hash table) cost one pointer bump
 * each and are freed together. Not thread-safe: one arena per thread.
 */
#ifndef FLOWPIPE_ARENA_H
#define FLOWPIPE_ARENA_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct fp_arena_block;

typedef struct fp_arena {
	struct fp_arena_block *head;	/* current block */
	size_t block_size;		/* default size of new blocks */
	size_t used;			/* bytes handed out */
	size_t reserved;		/* bytes obtained from malloc */
} fp_arena;

void fp_arena_init(fp_arena *a, size_t block_size);
/* @align must be a power of two. NULL on allocation failure. */
void *fp_arena_alloc(fp_arena *a, size_t size, size_t align);
/* Copy @len bytes and NUL-terminate. */
char *fp_arena_strndup(fp_arena *a, const char *s, size_t len);
/* Release every block. The arena can be reused afterwards. */
void fp_arena_free(fp_arena *a);

#ifdef __cplusplus
}
#endif

#endif /* FLOWPIPE_ARENA_H */
