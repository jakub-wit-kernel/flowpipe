// SPDX-License-Identifier: MIT
#define _GNU_SOURCE
#include "flowpipe/pool.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct task {
	fp_task_fn fn;
	void *arg;
};

struct worker {
	fp_pool *pool;
	int idx;
	pthread_t thread;
	_Atomic uint64_t executed;
};

struct fp_pool {
	fp_queue *q;
	int nthreads;
	struct worker *w;

	/*
	 * Shutdown handshake. Submitters register in `submitters` before
	 * checking `accepting`; shutdown clears `accepting` and then waits for
	 * `submitters` to reach zero before closing the queue, so no push can
	 * race with the close (the queue's close contract).
	 */
	_Atomic bool accepting;
	_Atomic int submitters;
	_Atomic bool cancelling;
	fp_task_fn on_cancel;		/* written before `cancelling` is set */

	/* Idle tracking: tasks submitted and not yet finished or cancelled. */
	_Atomic uint64_t pending;
	pthread_mutex_t idle_lock;
	pthread_cond_t idle_cv;

	_Atomic uint64_t submitted, completed, cancelled;

	pthread_mutex_t ctl_lock;
	bool shut_down;
	size_t cancelled_at_shutdown;
};

static _Thread_local fp_pool *tls_pool;
static _Thread_local int tls_worker = -1;

int fp_pool_current_worker(void)
{
	return tls_worker;
}

static void task_finished(fp_pool *p)
{
	/* acq_rel: the task's effects happen-before a waiter seeing zero. */
	if (atomic_fetch_sub_explicit(&p->pending, 1,
				      memory_order_acq_rel) == 1) {
		pthread_mutex_lock(&p->idle_lock);
		pthread_cond_broadcast(&p->idle_cv);
		pthread_mutex_unlock(&p->idle_lock);
	}
}

static void *worker_main(void *arg)
{
	struct worker *w = arg;
	fp_pool *p = w->pool;
	struct task t;

	tls_pool = p;
	tls_worker = w->idx;
	while (fp_queue_pop(p->q, &t) == 0) {
		if (atomic_load_explicit(&p->cancelling, memory_order_acquire)) {
			if (p->on_cancel)
				p->on_cancel(t.arg);
			atomic_fetch_add_explicit(&p->cancelled, 1,
						  memory_order_relaxed);
		} else {
			t.fn(t.arg);
			atomic_fetch_add_explicit(&w->executed, 1,
						  memory_order_relaxed);
			atomic_fetch_add_explicit(&p->completed, 1,
						  memory_order_relaxed);
		}
		task_finished(p);
	}
	return NULL;
}

void fp_pool_config_init(struct fp_pool_config *cfg)
{
	long n = sysconf(_SC_NPROCESSORS_ONLN);

	cfg->threads = n > 0 ? (int)n : 1;
	cfg->queue_capacity = 1024;
	cfg->queue_kind = FP_QUEUE_LOCKFREE;
}

int fp_pool_create(fp_pool **out, const struct fp_pool_config *cfg)
{
	struct fp_pool_config c;
	sigset_t all, old;
	fp_pool *p;
	int ret;

	*out = NULL;
	fp_pool_config_init(&c);
	if (cfg)
		c = *cfg;
	if (c.threads < 1 || c.threads > 4096)
		return -EINVAL;

	p = calloc(1, sizeof(*p));
	if (!p)
		return -ENOMEM;
	ret = fp_queue_create(&p->q, c.queue_kind, c.queue_capacity,
			      sizeof(struct task));
	if (ret) {
		free(p);
		return ret;
	}
	p->w = calloc((size_t)c.threads, sizeof(*p->w));
	if (!p->w) {
		fp_queue_destroy(p->q);
		free(p);
		return -ENOMEM;
	}
	atomic_init(&p->accepting, true);
	pthread_mutex_init(&p->idle_lock, NULL);
	pthread_cond_init(&p->idle_cv, NULL);
	pthread_mutex_init(&p->ctl_lock, NULL);

	/* Workers never run signal handlers; the application owns signals. */
	sigfillset(&all);
	pthread_sigmask(SIG_BLOCK, &all, &old);
	for (int i = 0; i < c.threads; i++) {
		p->w[i].pool = p;
		p->w[i].idx = i;
		ret = pthread_create(&p->w[i].thread, NULL, worker_main,
				     &p->w[i]);
		if (ret) {
			p->nthreads = i;
			pthread_sigmask(SIG_SETMASK, &old, NULL);
			fp_pool_destroy(p);
			return -ret;
		}
	}
	pthread_sigmask(SIG_SETMASK, &old, NULL);
	p->nthreads = c.threads;
	*out = p;
	return 0;
}

static int submit(fp_pool *p, fp_task_fn fn, void *arg, bool block)
{
	struct task t = { fn, arg };
	int ret;

	if (!fn)
		return -EINVAL;
	atomic_fetch_add(&p->submitters, 1);
	if (!atomic_load(&p->accepting)) {
		atomic_fetch_sub(&p->submitters, 1);
		return -EPIPE;
	}
	/* Count before pushing so wait_idle never sees a premature zero. */
	atomic_fetch_add_explicit(&p->pending, 1, memory_order_relaxed);
	ret = block ? fp_queue_push(p->q, &t) : fp_queue_try_push(p->q, &t);
	if (ret == 0)
		atomic_fetch_add_explicit(&p->submitted, 1,
					  memory_order_relaxed);
	else
		task_finished(p);	/* undo the pending count */
	atomic_fetch_sub(&p->submitters, 1);
	return ret;
}

int fp_pool_submit(fp_pool *p, fp_task_fn fn, void *arg)
{
	return submit(p, fn, arg, true);
}

int fp_pool_try_submit(fp_pool *p, fp_task_fn fn, void *arg)
{
	return submit(p, fn, arg, false);
}

int fp_pool_wait_idle(fp_pool *p)
{
	if (tls_pool == p)
		return -EDEADLK;
	pthread_mutex_lock(&p->idle_lock);
	while (atomic_load_explicit(&p->pending, memory_order_acquire) != 0)
		pthread_cond_wait(&p->idle_cv, &p->idle_lock);
	pthread_mutex_unlock(&p->idle_lock);
	return 0;
}

size_t fp_pool_shutdown(fp_pool *p, enum fp_shutdown_mode mode,
			fp_task_fn on_cancel)
{
	pthread_mutex_lock(&p->ctl_lock);
	if (p->shut_down) {
		pthread_mutex_unlock(&p->ctl_lock);
		return p->cancelled_at_shutdown;
	}
	atomic_store(&p->accepting, false);
	/* Wait out submitters that passed the accepting check. A submitter
	 * blocked on a full queue makes progress as workers drain it. */
	while (atomic_load(&p->submitters) != 0)
		sched_yield();
	if (mode == FP_SHUTDOWN_CANCEL) {
		p->on_cancel = on_cancel;
		atomic_store_explicit(&p->cancelling, true,
				      memory_order_release);
	}
	fp_queue_close(p->q);
	for (int i = 0; i < p->nthreads; i++)
		pthread_join(p->w[i].thread, NULL);
	p->cancelled_at_shutdown = (size_t)atomic_load(&p->cancelled);
	p->shut_down = true;
	pthread_mutex_unlock(&p->ctl_lock);
	return p->cancelled_at_shutdown;
}

void fp_pool_destroy(fp_pool *p)
{
	if (!p)
		return;
	fp_pool_shutdown(p, FP_SHUTDOWN_DRAIN, NULL);
	fp_queue_destroy(p->q);
	pthread_mutex_destroy(&p->idle_lock);
	pthread_cond_destroy(&p->idle_cv);
	pthread_mutex_destroy(&p->ctl_lock);
	free(p->w);
	free(p);
}

int fp_pool_threads(const fp_pool *p)
{
	return p->nthreads;
}

void fp_pool_stats(const fp_pool *p, struct fp_pool_stats *out)
{
	fp_pool *m = (fp_pool *)p;

	out->submitted = atomic_load_explicit(&m->submitted, memory_order_relaxed);
	out->completed = atomic_load_explicit(&m->completed, memory_order_relaxed);
	out->cancelled = atomic_load_explicit(&m->cancelled, memory_order_relaxed);
}

uint64_t fp_pool_worker_executed(const fp_pool *p, int i)
{
	return atomic_load_explicit(&((fp_pool *)p)->w[i].executed,
				    memory_order_relaxed);
}
