// SPDX-License-Identifier: MIT
/*
 * Bounded MPMC queues. See include/flowpipe/queue.h and docs/ARCHITECTURE.md.
 */
#define _GNU_SOURCE
#include "flowpipe/queue.h"

#include <errno.h>
#include <limits.h>
#include <linux/futex.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

#define CACHELINE	64
#define SPIN_LIMIT	128	/* polls before sleeping */

static inline void cpu_relax(void)
{
#if defined(__x86_64__) || defined(__i386__)
	__builtin_ia32_pause();
#elif defined(__aarch64__)
	__asm__ __volatile__("yield");
#endif
}

/* ---- eventcount: "sleep until something changes" on top of a futex ---- */

/*
 * Usage.  Waiter:   key = ec_prepare(); if (condition) { ec_cancel(key); done; }
 *                   ec_wait(key);   -- sleeps unless notified since prepare
 *         Notifier: make condition true; ec_notify();
 *
 * State is one 64-bit word: high 32 bits = epoch, low 32 bits = number of
 * registered waiters. (Vyukov's eventcount design.)
 *
 *  - prepare: atomically register (waiters + 1) and remember the epoch.
 *  - notify:  if waiters > 0, CAS the word to (epoch + 1, 0): this claims
 *             every registered waiter at once, then one FUTEX_WAKE wakes
 *             them. Later notifies see waiters == 0 and make no syscall
 *             until somebody registers again. (A plain "waiters" counter
 *             made a producer issue a wake syscall on every push until the
 *             sleeping consumer got scheduled; measured in fpbench.)
 *  - cancel:  deregister only if our epoch is still current; if a notifier
 *             already claimed us, there is nothing to undo.
 *
 * Visibility is a Dekker handshake via seq_cst fences:
 *   waiter:   register ; fence ; re-check condition
 *   notifier: publish  ; fence ; read state
 * Either the waiter's re-check sees the element, or the notifier sees the
 * registration, bumps the epoch, and futex_wait(epoch, key) cannot sleep.
 *
 * The futex word is the epoch half of the 64-bit state.
 */
struct eventcount {
	_Atomic uint64_t state;
};

#define EC_WAITERS(s)	((uint32_t)(s))
#define EC_EPOCH(s)	((uint32_t)((s) >> 32))

static uint32_t *ec_futex_word(struct eventcount *ec)
{
	uint32_t *w = (uint32_t *)&ec->state;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
	return w + 1;
#else
	return w;
#endif
}

static uint32_t ec_prepare(struct eventcount *ec)
{
	uint64_t old = atomic_fetch_add(&ec->state, 1);

	atomic_thread_fence(memory_order_seq_cst);
	return EC_EPOCH(old);
}

static void ec_cancel(struct eventcount *ec, uint32_t key)
{
	uint64_t s = atomic_load(&ec->state);

	/* Still registered in epoch `key`? Then remove ourselves. */
	while (EC_EPOCH(s) == key &&
	       !atomic_compare_exchange_weak(&ec->state, &s, s - 1))
		;
}

static void ec_wait(struct eventcount *ec, uint32_t key)
{
	/* Returns immediately (EAGAIN) if the epoch already moved past key. */
	syscall(SYS_futex, ec_futex_word(ec), FUTEX_WAIT_PRIVATE, key, NULL,
		NULL, 0);
	ec_cancel(ec, key);	/* no-op if a notifier claimed us */
}

static void ec_notify(struct eventcount *ec)
{
	uint64_t s;

	atomic_thread_fence(memory_order_seq_cst);
	s = atomic_load_explicit(&ec->state, memory_order_relaxed);
	while (EC_WAITERS(s)) {
		uint64_t next = (uint64_t)(EC_EPOCH(s) + 1u) << 32;

		if (atomic_compare_exchange_weak(&ec->state, &s, next)) {
			/* Wake all claimed waiters: any of them may be the one
			 * able to make progress. */
			syscall(SYS_futex, ec_futex_word(ec), FUTEX_WAKE_PRIVATE,
				INT_MAX, NULL, NULL, 0);
			return;
		}
	}
}

/* ---- common header ----------------------------------------------------- */

struct fp_queue {
	enum fp_queue_kind kind;
	size_t elem_size;
	size_t capacity;

	/* lock-free ring */
	unsigned char *cells;
	size_t stride;			/* bytes per cell */
	size_t mask;
	_Alignas(CACHELINE) _Atomic size_t enq_pos;
	_Alignas(CACHELINE) _Atomic size_t deq_pos;
	_Alignas(CACHELINE) struct eventcount not_empty;
	struct eventcount not_full;
	_Atomic bool closed;

	/* mutex ring */
	_Alignas(CACHELINE) pthread_mutex_t lock;
	pthread_cond_t cv_not_empty, cv_not_full;
	unsigned char *ring;
	size_t head, count;
	bool mclosed;
};

const char *fp_queue_kind_name(enum fp_queue_kind kind)
{
	return kind == FP_QUEUE_LOCKFREE ? "lockfree" : "mutex";
}

/* ---- lock-free implementation (Vyukov bounded MPMC) ------------------- */

static inline _Atomic size_t *cell_seq(fp_queue *q, size_t pos)
{
	return (_Atomic size_t *)(q->cells + (pos & q->mask) * q->stride);
}

static inline unsigned char *cell_data(fp_queue *q, size_t pos)
{
	return q->cells + (pos & q->mask) * q->stride + sizeof(size_t);
}

/*
 * Each cell carries a sequence number. For position pos:
 *   seq == pos       cell is free for the producer that claims pos
 *   seq == pos + 1   cell holds the element for the consumer of pos
 * A producer claims pos with one CAS on enq_pos, writes the payload, then
 * publishes it with a release store of seq = pos + 1. The consumer frees
 * the cell for the next lap with seq = pos + capacity.
 */
static int lf_try_push(fp_queue *q, const void *elem)
{
	size_t pos = atomic_load_explicit(&q->enq_pos, memory_order_relaxed);

	for (;;) {
		size_t seq = atomic_load_explicit(cell_seq(q, pos),
						  memory_order_acquire);
		intptr_t diff = (intptr_t)seq - (intptr_t)pos;

		if (diff == 0) {
			if (atomic_compare_exchange_weak_explicit(
				    &q->enq_pos, &pos, pos + 1,
				    memory_order_relaxed, memory_order_relaxed))
				break;
			/* pos was reloaded by the failed CAS */
		} else if (diff < 0) {
			return -EAGAIN;		/* a full lap behind: full */
		} else {
			pos = atomic_load_explicit(&q->enq_pos,
						   memory_order_relaxed);
		}
	}
	memcpy(cell_data(q, pos), elem, q->elem_size);
	atomic_store_explicit(cell_seq(q, pos), pos + 1, memory_order_release);
	return 0;
}

static int lf_try_pop(fp_queue *q, void *out)
{
	size_t pos = atomic_load_explicit(&q->deq_pos, memory_order_relaxed);

	for (;;) {
		size_t seq = atomic_load_explicit(cell_seq(q, pos),
						  memory_order_acquire);
		intptr_t diff = (intptr_t)seq - (intptr_t)(pos + 1);

		if (diff == 0) {
			if (atomic_compare_exchange_weak_explicit(
				    &q->deq_pos, &pos, pos + 1,
				    memory_order_relaxed, memory_order_relaxed))
				break;
		} else if (diff < 0) {
			return -EAGAIN;		/* not yet published: empty */
		} else {
			pos = atomic_load_explicit(&q->deq_pos,
						   memory_order_relaxed);
		}
	}
	memcpy(out, cell_data(q, pos), q->elem_size);
	atomic_store_explicit(cell_seq(q, pos), pos + q->mask + 1,
			      memory_order_release);
	return 0;
}

static int lf_push(fp_queue *q, const void *elem, bool block)
{
	for (;;) {
		if (atomic_load_explicit(&q->closed, memory_order_acquire))
			return -EPIPE;
		for (int spin = 0; spin < (block ? SPIN_LIMIT : 1); spin++) {
			if (lf_try_push(q, elem) == 0) {
				ec_notify(&q->not_empty);
				return 0;
			}
			cpu_relax();
		}
		if (!block)
			return -EAGAIN;

		uint32_t key = ec_prepare(&q->not_full);

		if (atomic_load(&q->closed)) {
			ec_cancel(&q->not_full, key);
			return -EPIPE;
		}
		if (lf_try_push(q, elem) == 0) {
			ec_cancel(&q->not_full, key);
			ec_notify(&q->not_empty);
			return 0;
		}
		ec_wait(&q->not_full, key);
	}
}

static int lf_pop(fp_queue *q, void *out, bool block)
{
	for (;;) {
		for (int spin = 0; spin < (block ? SPIN_LIMIT : 1); spin++) {
			if (lf_try_pop(q, out) == 0) {
				ec_notify(&q->not_full);
				return 0;
			}
			cpu_relax();
		}

		uint32_t key = ec_prepare(&q->not_empty);

		if (lf_try_pop(q, out) == 0) {
			ec_cancel(&q->not_empty, key);
			ec_notify(&q->not_full);
			return 0;
		}
		/*
		 * Closed and still empty: drained. Elements pushed before the
		 * close (release) are visible to this acquire load, so the
		 * re-check above has already seen them.
		 */
		if (atomic_load(&q->closed)) {
			ec_cancel(&q->not_empty, key);
			return lf_try_pop(q, out) == 0 ? 0 : -EPIPE;
		}
		if (!block) {
			ec_cancel(&q->not_empty, key);
			return -EAGAIN;
		}
		ec_wait(&q->not_empty, key);
	}
}

/* ---- mutex implementation ----------------------------------------------- */

static int mx_push(fp_queue *q, const void *elem, bool block)
{
	pthread_mutex_lock(&q->lock);
	while (!q->mclosed && q->count == q->capacity) {
		if (!block) {
			pthread_mutex_unlock(&q->lock);
			return -EAGAIN;
		}
		pthread_cond_wait(&q->cv_not_full, &q->lock);
	}
	if (q->mclosed) {
		pthread_mutex_unlock(&q->lock);
		return -EPIPE;
	}
	memcpy(q->ring + ((q->head + q->count) % q->capacity) * q->elem_size,
	       elem, q->elem_size);
	q->count++;
	pthread_cond_signal(&q->cv_not_empty);
	pthread_mutex_unlock(&q->lock);
	return 0;
}

static int mx_pop(fp_queue *q, void *out, bool block)
{
	pthread_mutex_lock(&q->lock);
	while (q->count == 0) {
		if (q->mclosed || !block) {
			int r = q->mclosed ? -EPIPE : -EAGAIN;

			pthread_mutex_unlock(&q->lock);
			return r;
		}
		pthread_cond_wait(&q->cv_not_empty, &q->lock);
	}
	memcpy(out, q->ring + q->head * q->elem_size, q->elem_size);
	q->head = (q->head + 1) % q->capacity;
	q->count--;
	pthread_cond_signal(&q->cv_not_full);
	pthread_mutex_unlock(&q->lock);
	return 0;
}

/* ---- public API ----------------------------------------------------------- */

int fp_queue_create(fp_queue **out, enum fp_queue_kind kind, size_t capacity,
		    size_t elem_size)
{
	fp_queue *q;

	*out = NULL;
	if (capacity < 2 || capacity > ((size_t)1 << 30) || elem_size == 0 ||
	    (kind != FP_QUEUE_LOCKFREE && kind != FP_QUEUE_MUTEX))
		return -EINVAL;
	if (posix_memalign((void **)&q, CACHELINE, sizeof(*q)))
		return -ENOMEM;
	memset(q, 0, sizeof(*q));
	q->kind = kind;
	q->elem_size = elem_size;

	if (kind == FP_QUEUE_LOCKFREE) {
		size_t cap = 2;

		while (cap < capacity)
			cap <<= 1;
		q->capacity = cap;
		q->mask = cap - 1;
		q->stride = (sizeof(size_t) + elem_size + 7) & ~(size_t)7;
		if (posix_memalign((void **)&q->cells, CACHELINE,
				   cap * q->stride)) {
			free(q);
			return -ENOMEM;
		}
		for (size_t i = 0; i < cap; i++)
			atomic_init(cell_seq(q, i), i);
		atomic_init(&q->enq_pos, 0);
		atomic_init(&q->deq_pos, 0);
		atomic_init(&q->closed, false);
	} else {
		q->capacity = capacity;
		q->ring = malloc(capacity * elem_size);
		if (!q->ring) {
			free(q);
			return -ENOMEM;
		}
		pthread_mutex_init(&q->lock, NULL);
		pthread_cond_init(&q->cv_not_empty, NULL);
		pthread_cond_init(&q->cv_not_full, NULL);
	}
	*out = q;
	return 0;
}

void fp_queue_destroy(fp_queue *q)
{
	if (!q)
		return;
	if (q->kind == FP_QUEUE_MUTEX) {
		pthread_mutex_destroy(&q->lock);
		pthread_cond_destroy(&q->cv_not_empty);
		pthread_cond_destroy(&q->cv_not_full);
		free(q->ring);
	} else {
		free(q->cells);
	}
	free(q);
}

int fp_queue_push(fp_queue *q, const void *elem)
{
	return q->kind == FP_QUEUE_LOCKFREE ? lf_push(q, elem, true) :
					      mx_push(q, elem, true);
}

int fp_queue_pop(fp_queue *q, void *out)
{
	return q->kind == FP_QUEUE_LOCKFREE ? lf_pop(q, out, true) :
					      mx_pop(q, out, true);
}

int fp_queue_try_push(fp_queue *q, const void *elem)
{
	return q->kind == FP_QUEUE_LOCKFREE ? lf_push(q, elem, false) :
					      mx_push(q, elem, false);
}

int fp_queue_try_pop(fp_queue *q, void *out)
{
	return q->kind == FP_QUEUE_LOCKFREE ? lf_pop(q, out, false) :
					      mx_pop(q, out, false);
}

void fp_queue_close(fp_queue *q)
{
	if (q->kind == FP_QUEUE_LOCKFREE) {
		atomic_store_explicit(&q->closed, true, memory_order_release);
		ec_notify(&q->not_empty);
		ec_notify(&q->not_full);
		return;
	}
	pthread_mutex_lock(&q->lock);
	q->mclosed = true;
	pthread_cond_broadcast(&q->cv_not_empty);
	pthread_cond_broadcast(&q->cv_not_full);
	pthread_mutex_unlock(&q->lock);
}

size_t fp_queue_capacity(const fp_queue *q)
{
	return q->capacity;
}

size_t fp_queue_size(const fp_queue *q)
{
	if (q->kind == FP_QUEUE_LOCKFREE) {
		size_t e = atomic_load_explicit(&((fp_queue *)q)->enq_pos,
						memory_order_relaxed);
		size_t d = atomic_load_explicit(&((fp_queue *)q)->deq_pos,
						memory_order_relaxed);

		return e > d ? (e - d > q->capacity ? q->capacity : e - d) : 0;
	}
	pthread_mutex_lock(&((fp_queue *)q)->lock);
	size_t n = q->count;

	pthread_mutex_unlock(&((fp_queue *)q)->lock);
	return n;
}
