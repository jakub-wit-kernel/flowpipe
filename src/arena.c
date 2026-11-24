// SPDX-License-Identifier: MIT
#include "flowpipe/arena.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct fp_arena_block {
	struct fp_arena_block *prev;
	size_t size;	/* usable bytes in data[] */
	size_t off;
	_Alignas(16) unsigned char data[];
};

void fp_arena_init(fp_arena *a, size_t block_size)
{
	memset(a, 0, sizeof(*a));
	a->block_size = block_size ? block_size : (size_t)64 * 1024;
}

void *fp_arena_alloc(fp_arena *a, size_t size, size_t align)
{
	struct fp_arena_block *b = a->head;
	size_t off;

	if (align == 0 || (align & (align - 1)) || align > 16)
		return NULL;
	if (b) {
		off = (b->off + align - 1) & ~(align - 1);
		if (off <= b->size && size <= b->size - off) {
			b->off = off + size;
			a->used += size;
			return b->data + off;
		}
	}
	/* New block; oversized requests get a block of their own size. */
	size_t bsize = size > a->block_size ? size : a->block_size;

	if (bsize > SIZE_MAX - sizeof(*b))
		return NULL;
	b = malloc(sizeof(*b) + bsize);
	if (!b)
		return NULL;
	b->prev = a->head;
	b->size = bsize;
	b->off = size;
	a->head = b;
	a->reserved += sizeof(*b) + bsize;
	a->used += size;
	return b->data;
}

char *fp_arena_strndup(fp_arena *a, const char *s, size_t len)
{
	char *p = fp_arena_alloc(a, len + 1, 1);

	if (p) {
		memcpy(p, s, len);
		p[len] = '\0';
	}
	return p;
}

void fp_arena_free(fp_arena *a)
{
	struct fp_arena_block *b = a->head;

	while (b) {
		struct fp_arena_block *prev = b->prev;

		free(b);
		b = prev;
	}
	a->head = NULL;
	a->used = 0;
	a->reserved = 0;
}
