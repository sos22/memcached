/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <assert.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>

#include "ae.h"
#include "zmalloc.h"
#include "config.h"

#include "ae_epoll.c"

#define DBG(...) do {} while (0)

aeEventLoop *aeCreateEventLoop(void) {
    aeEventLoop *eventLoop;
    int i;

    eventLoop = zmalloc(sizeof(*eventLoop));
    if (!eventLoop) return NULL;
    memset(eventLoop, 0, sizeof(*eventLoop));
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    if (aeApiCreate(eventLoop) == -1) {
        zfree(eventLoop);
        return NULL;
    }
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    for (i = 0; i < AE_SETSIZE; i++) {
        eventLoop->events[i].ll_mask = AE_NONE;
        eventLoop->events[i].hl_mask = AE_NONE;
    }
    return eventLoop;
}

void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

int aeCreateFileEvent(aeEventLoop *eventLoop, struct fable_handle *sfd, int mask,
		      aeFileProc *proc, void *clientData)
{
    DBG("aeCreateFileEvent(%p, %d)\n", (void *)sfd, mask);
    if (mask & AE_READABLE) {
	int r;
	int fd = fable_get_fd_read(sfd, &r);
	assert(fd < AE_SETSIZE);
	aeFileEvent *fe = &eventLoop->events[fd];

	if (aeApiAddEvent(eventLoop, fd, r ? AE_READABLE : AE_WRITABLE) == -1)
	    abort();
	fe->handle = sfd;
	fe->hl_mask |= AE_READABLE;
	fe->rfileProc = proc;
	fe->clientData = clientData;
	if (fd > eventLoop->maxfd)
	    eventLoop->maxfd = fd;

	if (fable_handle_is_readable(sfd))
	    aeApiFireEvent(eventLoop, fd, r ? AE_READABLE : AE_WRITABLE);
    }
    if (mask & AE_WRITABLE) {
	int r;
	int fd = fable_get_fd_write(sfd, &r);
	assert(fd < AE_SETSIZE);
	aeFileEvent *fe = &eventLoop->events[fd];

	if (aeApiAddEvent(eventLoop, fd, r ? AE_READABLE : AE_WRITABLE) == -1)
	    abort();
	fe->handle = sfd;
	fe->hl_mask |= AE_WRITABLE;
	fe->wfileProc = proc;
	fe->clientData = clientData;
	if (fd > eventLoop->maxfd)
	    eventLoop->maxfd = fd;
	if (fable_handle_is_writable(sfd))
	    aeApiFireEvent(eventLoop, fd, r ? AE_READABLE : AE_WRITABLE);
    }
    return AE_OK;
}

void aeDeleteFileEvent(aeEventLoop *eventLoop, struct fable_handle *sfd, int mask)
{
    DBG("aeDeleteFileEvent(%p, %d)\n", (void *)sfd, mask);
    if (mask & AE_READABLE) {
	int r;
	int fd = fable_get_fd_read(sfd, &r);
	aeFileEvent *fe = &eventLoop->events[fd];

	if (fe->hl_mask & AE_READABLE) {
	    fe->hl_mask = fe->hl_mask & ~AE_READABLE;
	    if (fd == eventLoop->maxfd &&
		fe->hl_mask == AE_NONE) {
		/* Update the max fd */
		int j;

		for (j = eventLoop->maxfd-1; j >= 0; j--)
		    if (eventLoop->events[j].hl_mask != AE_NONE) break;
		eventLoop->maxfd = j;
	    }
	    aeApiDelEvent(eventLoop, fd, r ? AE_READABLE : AE_WRITABLE);
	}
    }
    if (mask & AE_WRITABLE) {
	int r;
	int fd = fable_get_fd_write(sfd, &r);
	aeFileEvent *fe = &eventLoop->events[fd];

	DBG("Delete write event on fd %d\n", fd);
	if (fe->hl_mask & AE_WRITABLE) {
	    fe->hl_mask = fe->hl_mask & ~AE_WRITABLE;
	    if (fd == eventLoop->maxfd &&
		fe->hl_mask == AE_NONE) {
		/* Update the max fd */
		int j;

		for (j = eventLoop->maxfd-1; j >= 0; j--)
		    if (eventLoop->events[j].hl_mask != AE_NONE) break;
		eventLoop->maxfd = j;
	    }
	    aeApiDelEvent(eventLoop, fd, r ? AE_READABLE : AE_WRITABLE);
	}
    }
}

static void aeGetTime(long *seconds, long *milliseconds)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec/1000;
}

static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms) {
    long cur_sec, cur_ms, when_sec, when_ms;

    aeGetTime(&cur_sec, &cur_ms);
    when_sec = cur_sec + milliseconds/1000;
    when_ms = cur_ms + milliseconds%1000;
    if (when_ms >= 1000) {
        when_sec ++;
        when_ms -= 1000;
    }
    *sec = when_sec;
    *ms = when_ms;
}

long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;
    te->id = id;
    aeAddMillisecondsToNow(milliseconds,&te->when_sec,&te->when_ms);
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->next = eventLoop->timeEventHead;
    eventLoop->timeEventHead = te;
    return id;
}

int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    aeTimeEvent *te, *prev = NULL;

    te = eventLoop->timeEventHead;
    while(te) {
        if (te->id == id) {
            if (prev == NULL)
                eventLoop->timeEventHead = te->next;
            else
                prev->next = te->next;
            if (te->finalizerProc)
                te->finalizerProc(eventLoop, te->clientData);
            zfree(te);
            return AE_OK;
        }
        prev = te;
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* Search the first timer to fire.
 * This operation is useful to know how many time the select can be
 * put in sleep without to delay any event.
 * If there are no timers NULL is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    aeTimeEvent *nearest = NULL;

    while(te) {
        if (!nearest || te->when_sec < nearest->when_sec ||
                (te->when_sec == nearest->when_sec &&
                 te->when_ms < nearest->when_ms))
            nearest = te;
        te = te->next;
    }
    return nearest;
}

/* Process time events */
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;

    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId-1;
    while(te) {
        long now_sec, now_ms;
        long long id;

        if (te->id > maxId) {
            te = te->next;
            continue;
        }
        aeGetTime(&now_sec, &now_ms);
        if (now_sec > te->when_sec ||
            (now_sec == te->when_sec && now_ms >= te->when_ms))
        {
            int retval;

            id = te->id;
            retval = te->timeProc(eventLoop, id, te->clientData);
            processed++;
            /* After an event is processed our time event list may
             * no longer be the same, so we restart from head.
             * Still we make sure to don't process events registered
             * by event handlers itself in order to don't loop forever.
             * To do so we saved the max ID we want to handle.
             *
             * FUTURE OPTIMIZATIONS:
             * Note that this is NOT great algorithmically. Redis uses
             * a single time event so it's not a problem but the right
             * way to do this is to add the new elements on head, and
             * to flag deleted elements in a special way for later
             * deletion (putting references to the nodes to delete into
             * another linked list). */
            if (retval != AE_NOMORE) {
                aeAddMillisecondsToNow(retval,&te->when_sec,&te->when_ms);
            } else {
                aeDeleteTimeEvent(eventLoop, id);
            }
            te = eventLoop->timeEventHead;
        } else {
            te = te->next;
        }
    }
    return processed;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurrs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * the events that's possible to process without to wait are processed.
 *
 * The function returns the number of events processed. */
static int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0;

    /* Nothing to do? return ASAP */
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire. */
    if (eventLoop->fired_producer == eventLoop->fired_consumer &&
	(eventLoop->maxfd != -1 ||
	 ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT)))) {
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;

        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            shortest = aeSearchNearestTimer(eventLoop);
        if (shortest) {
            long now_sec, now_ms;

            /* Calculate the time missing for the nearest
             * timer to fire. */
            aeGetTime(&now_sec, &now_ms);
            tvp = &tv;
            tvp->tv_sec = shortest->when_sec - now_sec;
            if (shortest->when_ms < now_ms) {
                tvp->tv_usec = ((shortest->when_ms+1000) - now_ms)*1000;
                tvp->tv_sec --;
            } else {
                tvp->tv_usec = (shortest->when_ms - now_ms)*1000;
            }
            if (tvp->tv_sec < 0) tvp->tv_sec = 0;
            if (tvp->tv_usec < 0) tvp->tv_usec = 0;
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to se the timeout
             * to zero */
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                tvp = NULL; /* wait forever */
            }
        }

        aeApiPoll(eventLoop, tvp);
    }

    while (eventLoop->fired_consumer != eventLoop->fired_producer) {
      int idx = eventLoop->fired_consumer % AE_SETSIZE;
      DBG("FD %d fired, mask %d\n",
	  eventLoop->fired[idx].fd,
	  eventLoop->fired[idx].mask);
      aeFileEvent *fe = &eventLoop->events[eventLoop->fired[idx].fd];
      int ll_mask = fe->ll_mask;
      int hl_mask = fe->hl_mask;
      int fired_mask = eventLoop->fired[idx].mask;

      ll_mask &= fired_mask;
      if (ll_mask == AE_READABLE) {
	if (hl_mask == AE_READABLE) {
	  do {
	    fe->rfileProc(eventLoop,fe->handle,fe->clientData,AE_READABLE);
	  } while (fe->hl_mask == AE_READABLE && fable_handle_is_readable(fe->handle));
	} else if (hl_mask == AE_WRITABLE) {
	  do {
	    fe->wfileProc(eventLoop,fe->handle,fe->clientData,AE_WRITABLE);
	  } while (fe->hl_mask == AE_WRITABLE &&
		   fable_handle_is_writable(fe->handle));
	} else {
	  abort();
	}
      } else if (ll_mask == AE_WRITABLE) {
	assert(hl_mask & AE_WRITABLE);
	do {
	  fe->wfileProc(eventLoop,fe->handle,fe->clientData,AE_WRITABLE);
	} while (fe->hl_mask == AE_WRITABLE &&
		 fable_handle_is_writable(fe->handle));
      } else if (ll_mask == 0) {
      } else {
	abort();
      }
      processed++;

      eventLoop->fired_consumer++;
    }


    /* Check time events */
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed; /* return the number of processed file/time events */
}

void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    while (!eventLoop->stop) {
        if (eventLoop->beforesleep != NULL)
            eventLoop->beforesleep(eventLoop);
        aeProcessEvents(eventLoop, AE_ALL_EVENTS);
    }
}
