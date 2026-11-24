/* SPDX-License-Identifier: MIT */
/*
 * fp_queue - bounded multi-producer / multi-consumer FIFO of fixed-size
 * elements, with blocking and non-blocking operations and close semantics.
 *
 * Two interchangeable implementations:
 *   FP_QUEUE_LOCKFREE  Vyukov's bounded MPMC ring (one CAS per operation,
 *                      no locks); blocking waits use a futex eventcount.
 *   FP_QUEUE_MUTEX     ring buffer + mutex + two condition variables.
 *
 * Close protocol: after fp_queue_close(), pushes fail with -EPIPE and pops
 * drain the remaining elements, then fail with -EPIPE. Close must not race
 * with in-flight pushes the caller expects to be delivered (close after the
 * producers are done, as a "last producer closes" rule).
 */
#ifndef FLOWPIPE_QUEUE_H
#define FLOWPIPE_QUEUE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fp_queue fp_queue;

enum fp_queue_kind {
	FP_QUEUE_LOCKFREE,
	FP_QUEUE_MUTEX,
};

/* @capacity is rounded up to a power of two (lock-free) and must be >= 2. */
int fp_queue_create(fp_queue **out, enum fp_queue_kind kind, size_t capacity,
		    size_t elem_size);
void fp_queue_destroy(fp_queue *q);

/* Blocking. 0 on success, -EPIPE once closed. */
int fp_queue_push(fp_queue *q, const void *elem);
int fp_queue_pop(fp_queue *q, void *out);

/* Non-blocking. 0, -EAGAIN (full/empty) or -EPIPE (closed / closed+empty). */
int fp_queue_try_push(fp_queue *q, const void *elem);
int fp_queue_try_pop(fp_queue *q, void *out);

void fp_queue_close(fp_queue *q);
size_t fp_queue_capacity(const fp_queue *q);
/* Approximate under concurrency. */
size_t fp_queue_size(const fp_queue *q);
const char *fp_queue_kind_name(enum fp_queue_kind kind);

#ifdef __cplusplus
}
#endif

#endif /* FLOWPIPE_QUEUE_H */
