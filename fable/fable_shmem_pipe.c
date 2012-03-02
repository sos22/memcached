/* Another kind of shared memory test.  The idea here is that we have
   a shared-memory region and a malloc-like thing for allocating from
   it, and we also have a couple of pipes.  Messages are sent by
   allocating a chunk of the shared region and then sending an extent
   through the pipe.  Once the receiver is finished with the message,
   they send another extent back through the other pipe saying that
   they're done. */
#undef NDEBUG
#define _GNU_SOURCE
#include <sys/poll.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <err.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <math.h>

#include "../libevent/event.h"

#include "fable.h"
#include "io_helpers.h"

#define container_of(ptr, type, field)				\
  ((type *)((unsigned long)(ptr) - offsetof(type, field)))

#define PAGE_ORDER 12
#define CACHE_LINE_SIZE 64
static unsigned ring_order = 9;
#define ring_size (1ul << (PAGE_ORDER + ring_order))

#define PAGE_SIZE 4096
#define EXTENT_BUFFER_SIZE 4096

#define ALLOC_FAILED ((unsigned)-1)

#ifdef NDEBUG
#define DBG(x) do {} while (0)
#else
#define DBG(x) do { x; } while (0)
#endif

//#define DBGPRINT printf
#define DBGPRINT(...) do {} while (0)

/* The extents which we poke into the ancillary pipe can represent
   either data to be consumed by the receiver or buffers which have
   been released by the sender. */
struct extent {
  unsigned base1:30;
#define EXTENT_TYPE_RELEASE 1
#define EXTENT_TYPE_DATA 2
  unsigned type:2;
  unsigned size;
};

typedef struct {
  int is_send;
} simplex_id_t;

#define SIMPLEX_SEND ((simplex_id_t){1})
#define SIMPLEX_RECV ((simplex_id_t){0})

#define MAX_ALLOC_NODES 50

struct shmem_simplex {
        simplex_id_t simpl_id;
	void *ring;
        int ring_pages;
	struct alloc_node *first_alloc, *next_free_alloc, *last_freed_node;
#ifndef NDEBUG
	int nr_alloc_nodes;
#endif
};

struct shmem_handle {
  int fd;

  /* Buffering on the file descriptor */

  unsigned char rx_buf[EXTENT_BUFFER_SIZE]; /* Stuff we've received from the other end */
  unsigned rx_buf_prod;
  unsigned rx_buf_cons;

  /* Messages which we've got queued up to send to the other side.
     Most of the time, this will just contain release messages, but we
     can also marshall send messages in here during send
     operations. */
  struct extent outgoing_extents[EXTENT_BUFFER_SIZE/sizeof(struct extent) + 1];
  unsigned nr_outgoing_extents;
  unsigned outgoing_extent_bytes;

  char *name;
  struct shmem_simplex *send;
  struct shmem_simplex *recv;
};

struct shmem_pipe_buf {

  struct fable_buf base;
  struct iovec iov;

};

/* Our allocation structure is a simple linked list.  That's pretty
   stupid, *except* that the allocation pattern is almost always a
   very simple queue, so it becomes very simple.  i.e. we release
   stuff in a FIFO order wrt allocations, so we effectively just have
   one allocated region which loops around the shared area, which
   makes the linked list very short and everything is very easy. */
struct alloc_node {
	struct alloc_node *next, *prev;
	int is_free;
	unsigned long start;
	unsigned long end;
};

#ifndef NDEBUG
static void
sanity_check(const struct shmem_simplex *sp)
{
	const struct alloc_node *cursor;
	int found_nf = 0, found_lf = 0;
	int n = 0;
	assert(sp->first_alloc);
	assert(sp->first_alloc->start == 0);
	for (cursor = sp->first_alloc;
	     cursor;
	     cursor = cursor->next) {
		n++;
		if (cursor == sp->first_alloc)
			assert(!cursor->prev);
		else
			assert(cursor->prev);
		if (cursor->next)
			assert(cursor->next->prev == cursor);
		if (cursor->prev)
			assert(cursor->start == cursor->prev->end);
		if (cursor->prev)
			assert(cursor->is_free == !cursor->prev->is_free);
		if (!cursor->next)
			assert(cursor->end == ring_size);
		if (cursor == sp->next_free_alloc) {
			assert(!found_nf);
			found_nf = 1;
		}
		if (cursor == sp->last_freed_node) {
			assert(!found_lf);
			found_lf = 1;
		}
		assert(cursor->start < cursor->end);
	}
	if (!found_nf)
		assert(!sp->next_free_alloc);
	else
		assert(sp->next_free_alloc->is_free);
	if (!found_lf)
		assert(!sp->last_freed_node);
	else
		assert(sp->last_freed_node->is_free);
	assert(n == sp->nr_alloc_nodes);
}
#else
static void
sanity_check(const struct shmem_simplex *sp)
{
}
#endif

static unsigned any_shared_space(struct shmem_simplex *sp)
{

  /* As its name suggests, if next_free_alloc is set, then it points to a free area. We can write at least 1 byte, so we're writable. */
  if(sp->next_free_alloc)
    return 1;
  /* Search the linked list. Don't keep next_free_alloc as it'd make the alloc_shared take a silly decision when we stop at a 1-byte hole.  */

  struct alloc_node* n;
  for (n = sp->first_alloc; n && (!n->is_free); n = n->next) ;

  return (n != 0);

}

static unsigned
alloc_shared_space(struct shmem_simplex *sp, unsigned* size_out)
{
        unsigned size = *size_out;
	unsigned res;

	sanity_check(sp);

	/* Common case */
	if (sp->next_free_alloc &&
	    sp->next_free_alloc->end >= size + sp->next_free_alloc->start &&
	    sp->next_free_alloc->prev) {
	allocate_next_free:
		assert(!sp->next_free_alloc->prev->is_free);
		assert(sp->next_free_alloc->is_free);
		res = sp->next_free_alloc->start;
		sp->next_free_alloc->start += size;
		sp->next_free_alloc->prev->end += size;
		if (sp->next_free_alloc->start == sp->next_free_alloc->end) {
			if (sp->next_free_alloc->next) {
				assert(!sp->next_free_alloc->next->is_free);
				sp->next_free_alloc->prev->next = sp->next_free_alloc->next->next;
				sp->next_free_alloc->prev->end = sp->next_free_alloc->next->end;
				if (sp->next_free_alloc->next->next) {
					assert(sp->next_free_alloc->next->next->is_free);
					sp->next_free_alloc->next->next->prev = sp->next_free_alloc->prev;
				}
				struct alloc_node *p = sp->next_free_alloc->next->next;
				DBG(sp->nr_alloc_nodes--);
				free(sp->next_free_alloc->next);
				if (sp->next_free_alloc->next == sp->last_freed_node)
					sp->last_freed_node = NULL;
				sp->next_free_alloc->next = p;
			} else {
				if (sp->next_free_alloc->prev)
					sp->next_free_alloc->prev->next = NULL;
			}
			if (sp->first_alloc == sp->next_free_alloc) {
				assert(sp->next_free_alloc->next);
				assert(!sp->next_free_alloc->prev);
				sp->first_alloc = sp->next_free_alloc->next;
			}
			if (sp->next_free_alloc == sp->last_freed_node)
				sp->last_freed_node = NULL;
			DBG(sp->nr_alloc_nodes--);
			free(sp->next_free_alloc);
			sp->next_free_alloc = NULL;
		}
		sanity_check(sp);
		return res;
	}

	struct alloc_node *best_candidate = 0;
	unsigned int best_size = 0;

	/* Slightly harder case: have to search the linked list */
	for (sp->next_free_alloc = sp->first_alloc;
	     sp->next_free_alloc &&
		     (!sp->next_free_alloc->is_free || sp->next_free_alloc->end - sp->next_free_alloc->start < size);
	     sp->next_free_alloc = sp->next_free_alloc->next) {
	         
	  /* sp->next_free_alloc isn't enough or doesn't exist, but keep track of the next-best option */
	  if(sp->next_free_alloc && sp->next_free_alloc->is_free) {
	    unsigned int this_size = sp->next_free_alloc->end - sp->next_free_alloc->start;
	    if(this_size > best_size) {
	      best_size = this_size;
	      best_candidate = sp->next_free_alloc;
	    }
	  }

	}
	if (!sp->next_free_alloc) {
		/* Shared area has no gaps large enough, but in order to behave like a selectable device we
		   must return the next best candidate if there is one. */
	  if(best_candidate) {
	    sp->next_free_alloc = best_candidate;
	    (*size_out) = size = best_size;
	  }
	  else
	    return ALLOC_FAILED;
	}

	struct alloc_node *f = sp->next_free_alloc;
	assert(f->is_free);
	if (!f->prev) {
		/* Allocate the start of the arena. */
		assert(f->start == 0);
		assert(f == sp->first_alloc);
		if (f->end == size) {
			/* We're going to convert next_free_alloc to
			 * an in-use node.  This may involve forwards
			 * merging. */
			if (f->next) {
				struct alloc_node *t = f->next;
				assert(!t->is_free);
				f->end = t->end;
				f->next = t->next;
				if (f->next)
					f->next->prev = f;
				if (sp->last_freed_node == t)
					sp->last_freed_node = NULL;
				DBG(sp->nr_alloc_nodes--);
				free(t);
			}
			f->is_free = 0;
		} else {
		        f = calloc(sizeof(struct alloc_node), 1);
			DBG(sp->nr_alloc_nodes++);
			f->next = sp->first_alloc;
			f->start = 0;
			f->end = size;
			assert(f->next);
			f->next->prev = f;
			f->next->start = size;
			sp->first_alloc = f;
		}
		if (sp->last_freed_node == sp->first_alloc)
			sp->last_freed_node = sp->first_alloc->next;
		if (sp->next_free_alloc == sp->first_alloc)
			sp->next_free_alloc = sp->first_alloc->next;
		sanity_check(sp);
		return 0;
	} else {
		goto allocate_next_free;
	}
}

static void
release_shared_space(struct shmem_simplex *sp, unsigned start, unsigned size)
{
	DBGPRINT("Release [%x,%x) on %p\n", start, start+size, (void *)sp);
	struct alloc_node *lan = sp->last_freed_node;
	assert(start <= ring_size);
	assert(start + size <= ring_size);
	assert(size > 0);
	sanity_check(sp);
	if (lan &&
	    lan->is_free &&
	    lan->end == start) {
		struct alloc_node *next;
	free_from_here:
		next = lan->next;
		assert(next);
		assert(!next->is_free);
		assert(next->start == start);
		assert(next->end >= start + size);
		next->start += size;
		lan->end += size;
		if (next->start == next->end) {
			/* We just closed a hole.  Previously, we had
			   LAN->next->X, where LAN is sp->last_freed_node,
			   next is some free region, and X is either
			   NULL or some allocated region.  next is now
			   zero-sized, so we want to remove it and
			   convert to LAN->X.  However, LAN and X are
			   the same type (i.e. both non-free), so we
			   can extend LAN to cover X and remove X as
			   well. */
			struct alloc_node *X = next->next;

			if (X) {
				/* Convert LAN->next->X->Y into
				   LAN->Y */
				struct alloc_node *Y = X->next;
				assert(X->is_free);
				if (Y) {
					assert(!Y->is_free);
					Y->prev = lan;
				}
				lan->end = X->end;
				lan->next = Y;
				if (X == sp->next_free_alloc)
					sp->next_free_alloc = lan;
				DBG(sp->nr_alloc_nodes--);
				free(X);
			} else {
				/* Just turn LAN->free1->NULL into
				   LAN->NULL */
				assert(lan->end == next->start);
				lan->next = NULL;
			}
			if (next == sp->next_free_alloc)
				sp->next_free_alloc = lan;
			DBG(sp->nr_alloc_nodes--);
			free(next);
		}
		sanity_check(sp);
		return;
	}

	/* More tricky case: we're freeing something which doesn't hit
	 * the cache. */
	for (lan = sp->first_alloc;
	     lan && (lan->end <= start || lan->start > start);
	     lan = lan->next)
		;
	assert(lan); /* Or else we're freeing something outside of the arena */
	assert(!lan->is_free); /* Or we have a double free */
	if (lan->start == start) {
		/* Free out the start of this block. */
		assert(!lan->is_free);
		if (lan->prev) {
			assert(lan->prev->is_free);
			assert(lan->prev->end == start);
			sp->last_freed_node = lan = lan->prev;
			goto free_from_here;
		}
		/* Turn the very start of the arena into a free
		 * block */
		assert(lan == sp->first_alloc);
		assert(start == 0);
		if (lan->end == size) {
			/* Easy: just convert the existing node to a
			 * free one. */
			lan->is_free = 1;
			if (lan->next && lan->next->is_free) {
				/* First node is now free, and the
				   second node already was -> merge
				   them. */
				struct alloc_node *t = lan->next;
				lan->end = t->end;
				lan->next = t->next;
				if (lan->next)
					lan->next->prev = lan;
				if (sp->last_freed_node == t)
					sp->last_freed_node = lan;
				if (sp->next_free_alloc == t)
					sp->next_free_alloc = lan;
				DBG(sp->nr_alloc_nodes--);
				free(t);
			}
			sanity_check(sp);
		} else {
			/* Need a new node in the list */
      		        lan = calloc(sizeof(*lan), 1);
			lan->is_free = 1;
			lan->end = size;
			sp->first_alloc->start = lan->end;
			sp->first_alloc->prev = lan;
			lan->next = sp->first_alloc;
			sp->first_alloc = lan;
			sp->last_freed_node = sp->first_alloc;
			DBG(sp->nr_alloc_nodes++);
			sanity_check(sp);
		}
		return;
	}
	assert(start < lan->end);
	assert(start + size <= lan->end);
	if (start + size == lan->end) {
		/* Free out the end of this block */
		if (lan->next) {
			assert(lan->next->is_free);
			lan->next->start -= size;
			lan->end -= size;
			assert(lan->end != lan->start);
		} else {
			struct alloc_node *t = calloc(sizeof(*lan), 1);
			t->prev = lan;
			t->is_free = 1;
			t->start = start;
			t->end = start + size;
			lan->next = t;
			lan->end = start;
			DBG(sp->nr_alloc_nodes++);
		}
		if (!sp->next_free_alloc)
			sp->next_free_alloc = lan->next;
		sp->last_freed_node = lan->next;
		sanity_check(sp);
		return;
	}

	/* Okay, this is the tricky case.  We have a single allocated
	   node, and we need to convert it into three: an allocated
	   node, a free node, and then another allocated node.  How
	   tedious. */
	struct alloc_node *a = calloc(sizeof(*a), 1);
	struct alloc_node *b = calloc(sizeof(*b), 1);

	a->next = b;
	a->prev = lan;
	a->is_free = 1;
	a->start = start;
	a->end = start + size;

	b->next = lan->next;
	b->prev = a;
	b->is_free = 0;
	b->start = start + size;
	b->end = lan->end;

	if (lan->next)
		lan->next->prev = b;
	lan->next = a;
	lan->end = start;

	DBG(sp->nr_alloc_nodes += 2);

	if (!sp->next_free_alloc)
		sp->next_free_alloc = a;

	/* And we're done. */
	sanity_check(sp);
}

void
fable_init_shmem_pipe()
{
	char *_ring_order = getenv("SHMEM_RING_ORDER");
	if (_ring_order) {
		int as_int = -1;
		if (sscanf(_ring_order, "%d", &as_int) != 1)
		  fprintf(stderr, "SHMEM_RING_ORDER must be an integer; ignored");
		else {
		  if (as_int < 0 || as_int > 15)
		    fprintf(stderr, "SHMEM_RING_ORDER must be between 0 and 15; ignored");
		  else
		    ring_order = as_int;
		}
	}

	fable_init_unixdomain();
}

static struct shmem_simplex *
simplex_from_fable_handle(struct fable_handle *fh, simplex_id_t simpl)
{
  struct shmem_handle *sd = (struct shmem_handle *)fh;
  if (simpl.is_send)
    return sd->send;
  else
    return sd->recv;
}

static struct fable_handle *
fable_handle_from_shmem_simplexes(int fd,
				  struct shmem_simplex *send,
				  struct shmem_simplex *recv)
{
  struct shmem_handle *sd = calloc(sizeof(*sd), 1);
  sd->fd = fd;
  sd->send = send;
  sd->recv = recv;
  return (struct fable_handle *)sd;
}

struct fable_handle *
fable_listen_shmem_pipe(const char* name)
{
  void* unix_handle = fable_listen_unixdomain(name);
  if(!unix_handle)
    return NULL;
  int fd = *(int *)unix_handle;
  free(unix_handle);
  struct shmem_handle *sd = calloc(sizeof(*sd), 1);
  if (!sd) {
      close(fd);
      return NULL;
  }
  sd->fd = fd;
  return (struct fable_handle *)sd;
}

static void shmem_pipe_init_send(struct shmem_simplex* new_handle) {

  new_handle->first_alloc = (struct alloc_node*)calloc(sizeof(*new_handle->first_alloc), 1);
  new_handle->first_alloc->is_free = 1;
  new_handle->first_alloc->end = ring_size;
  DBG(new_handle->nr_alloc_nodes = 1);

}

static struct shmem_simplex *
accept_simplex(int fd, simplex_id_t simpl)
{
  // TODO: these are blocking operations, even if the Fable handle is nonblocking.
  int shmem_fd = unix_recv_fd(fd);
  if(shmem_fd == -1) {
    int save_errno = errno;
    errno = save_errno;
    return NULL;
  }

  int seg_pages;
  read_all_fd(fd, (char*)&seg_pages, sizeof(int));

  void* ring_addr = mmap(NULL, PAGE_SIZE * seg_pages, PROT_READ|PROT_WRITE, MAP_SHARED, shmem_fd, 0);
  close(shmem_fd);
  if(ring_addr == MAP_FAILED)
    return NULL;

  struct shmem_simplex* new_handle = calloc(sizeof(struct shmem_simplex), 1);
  if(!new_handle) {
    munmap(ring_addr, PAGE_SIZE * seg_pages);
    return NULL;
  }

  new_handle->ring = ring_addr;
  new_handle->ring_pages = seg_pages;
  new_handle->simpl_id = simpl;

  if (simpl.is_send)
    shmem_pipe_init_send(new_handle);
  // Otherwise memset-0 is enough

  return new_handle;
}

struct fable_handle *
fable_accept_shmem_pipe(struct fable_handle *handle, int direction)
{
  struct shmem_simplex *send, *recv;
  struct fable_handle *res;
  struct shmem_handle *h = (struct shmem_handle *)handle;
  void* new_unix_handle;
  int fd;

  DBGPRINT("Called accept on %p\n", (void *)handle);
  new_unix_handle = fable_accept_unixdomain((struct fable_handle *)&h->fd, FABLE_DIRECTION_DUPLEX);
  if(!new_unix_handle) {
    DBGPRINT("Unix accept failed: %s\n", strerror(errno));
    return NULL;
  }
  fd = *((int*)new_unix_handle);
  free(new_unix_handle);

  send = NULL;
  recv = NULL;
  DBGPRINT("Accept on %p\n", (void *)handle);
  if (direction == FABLE_DIRECTION_DUPLEX || direction == FABLE_DIRECTION_RECEIVE) {
    recv = accept_simplex(fd, SIMPLEX_RECV);
    DBGPRINT("Receive fd %d on %p\n", fd, (void *)recv);
  }
  if (direction == FABLE_DIRECTION_DUPLEX || direction == FABLE_DIRECTION_SEND) {
    send = accept_simplex(fd, SIMPLEX_SEND);
    DBGPRINT("Send fd %d on %p\n", fd, (void *)send);
  }

  setnb_fd(fd);
  res = fable_handle_from_shmem_simplexes(fd, send, recv);
  DBGPRINT("accepted %p from %p\n", (void *)res, (void *)handle);
  return res;
}

static struct shmem_simplex *
connect_simplex(int fd, simplex_id_t simplex)
{
  // As the connection initiator, it's our responsibility to supply shared memory.

  char random_name[22];
  int ring_pages = 1 << ring_order;
  int shm_fd = -1;
  int i;

  errno = EEXIST;
  for(i = 0; i < 100 && shm_fd == -1 && errno == EEXIST; ++i) {
    strcpy(random_name, "/fable_segment_XXXXXX");
    if(!mktemp(random_name))
      break;
    shm_fd = shm_open(random_name, O_RDWR|O_CREAT|O_EXCL, 0600);
  }

  if(shm_fd == -1)
    return NULL;

  shm_unlink(random_name);
  if (ftruncate(shm_fd, PAGE_SIZE * ring_pages) < 0) {
    close(shm_fd);
    return NULL;
  }

  void* ring_addr = mmap(NULL, PAGE_SIZE * ring_pages, PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd, 0);

  // OK, send our partner the FD and the appropriate size to mmap.
  int send_ret = unix_send_fd(fd, shm_fd);
  close(shm_fd);

  if(send_ret <= 0) {
    munmap(ring_addr, PAGE_SIZE * ring_pages);
    return NULL;
  }

  write_all_fd(fd, (const char*)&ring_pages, sizeof(int), 0);

  struct shmem_simplex* conn_handle = calloc(sizeof(struct shmem_simplex), 1);

  conn_handle->ring = ring_addr;
  conn_handle->ring_pages = ring_pages;
  conn_handle->simpl_id = simplex;

  if (simplex.is_send)
    shmem_pipe_init_send(conn_handle);
  // Otherwise memset-0 is enough

  return conn_handle;
}

struct fable_handle* fable_connect_shmem_pipe(const char* name, int direction)
{
  struct shmem_simplex *send, *recv;
  struct fable_handle *res;
  int fd;
  void *unix_handle;

  send = NULL;
  recv = NULL;

  unix_handle = fable_connect_unixdomain(name, FABLE_DIRECTION_DUPLEX);
  if(!unix_handle)
    return NULL;
  fd = *((int*)unix_handle);
  free(unix_handle);

  if (direction == FABLE_DIRECTION_DUPLEX || direction == FABLE_DIRECTION_SEND)
    send = connect_simplex(fd, SIMPLEX_SEND);
  if (direction == FABLE_DIRECTION_DUPLEX || direction == FABLE_DIRECTION_RECEIVE)
    recv = connect_simplex(fd, SIMPLEX_RECV);

  setnb_fd(fd);

  res = fable_handle_from_shmem_simplexes(fd, send, recv);
  DBGPRINT("connected %p\n", (void *)res);
  return res;
}

static int fill_incoming_buffers(struct shmem_handle *sp)
{
  if (sp->rx_buf_cons == sp->rx_buf_prod)
    sp->rx_buf_cons = sp->rx_buf_prod = 0;
  while (sp->rx_buf_prod - sp->rx_buf_cons < sizeof(struct extent)) {

    if (sp->rx_buf_prod - sp->rx_buf_cons < sizeof(struct extent) ||
	sizeof(sp->rx_buf) - sp->rx_buf_prod < sizeof(struct extent)) {
      memmove(sp->rx_buf, sp->rx_buf + sp->rx_buf_cons, sp->rx_buf_prod - sp->rx_buf_cons);
      sp->rx_buf_prod -= sp->rx_buf_cons;
      sp->rx_buf_cons = 0;
    }

    int k = read(sp->fd, (char *)sp->rx_buf + sp->rx_buf_prod,
		 sizeof(sp->rx_buf) - sp->rx_buf_prod);
    if (k == 0) {
      DBGPRINT("read failed: socket empty\n");
      errno = 0;
      return 0;
    }
    if (k < 0) {
      if (errno != EAGAIN)
	DBGPRINT("read failed: error %s\n", strerror(errno));
      if(errno == ECONNRESET) {
	errno = 0;
	return 0;
      } else {
	return -1;
      }
    }
    sp->rx_buf_prod += k;
  }
  return 1;
}

static void flush_outgoing_extent_queue(struct shmem_handle *handle)
{
  // TODO: blocking operations regardless of nonblocking status.
  DBGPRINT("Flushing %d extents on %p\n", handle->nr_outgoing_extents,
	   (void *)handle);
  write_all_fd(handle->fd, handle->outgoing_extents, sizeof(struct extent) * handle->nr_outgoing_extents, 1);
  handle->nr_outgoing_extents = 0;
  handle->outgoing_extent_bytes = 0;
}

static void queue_extent_release(struct shmem_handle *sp, unsigned offset, unsigned len)
{
  // Queue it for transmission back to the writer
  struct extent *out;
  out = &sp->outgoing_extents[(int)sp->nr_outgoing_extents-1];
  assert(sp->nr_outgoing_extents == 0 ||
	 out->type != EXTENT_TYPE_DATA);
  /* Try to reuse previous outgoing extent */
  if (sp->nr_outgoing_extents != 0 && out->base1 + out->size == offset) {
    out->size += len;
  } else {
    out++;
    out->base1 = offset;
    out->size = len;
    out->type = EXTENT_TYPE_RELEASE;
    sp->nr_outgoing_extents++;
    assert(sp->nr_outgoing_extents < EXTENT_BUFFER_SIZE/sizeof(struct extent));
  }
  sp->outgoing_extent_bytes += len;

  // Send the queued extents, if the queue is big enough
  if (sp->outgoing_extent_bytes > ring_size / 8)
    flush_outgoing_extent_queue(sp);
}

/* Note: no automatic flush here!  If the caller wants the queue
   flushed, they have to do it themselves. */
static void queue_extent_transmit(struct shmem_handle *sp, unsigned offset, unsigned len)
{
  struct extent *out;
  if (sp->nr_outgoing_extents == 0)
    goto fresh_extent;
  out = &sp->outgoing_extents[(int)sp->nr_outgoing_extents-1];
  if (out->type == EXTENT_TYPE_RELEASE ||
      out->base1 + out->size != offset)
    goto fresh_extent;
  out->size += len;
  return;

 fresh_extent:
  out = &sp->outgoing_extents[sp->nr_outgoing_extents];
  sp->nr_outgoing_extents++;
  out->base1 = offset;
  out->size = len;
  out->type = EXTENT_TYPE_DATA;

  return;
}

static struct extent *get_incoming_data_extent(struct shmem_handle *handle)
{
  struct extent *inc;

  while (1) {
    if (fill_incoming_buffers(handle) <= 0)
      return NULL;

    while (handle->rx_buf_prod - handle->rx_buf_cons >= sizeof(struct extent)) {
      inc = (struct extent*)(handle->rx_buf + handle->rx_buf_cons);
      if (inc->type == EXTENT_TYPE_RELEASE)
	release_shared_space(handle->send, inc->base1, inc->size);
      else if (inc->type == EXTENT_TYPE_DATA && inc->size != 0)
	return inc;
      handle->rx_buf_cons += sizeof(struct extent);
    }
  }

}

struct fable_buf* fable_get_read_buf_shmem_pipe(struct fable_handle* _handle, unsigned len)
{
  DBGPRINT("get read buf on %p\n", (void *)_handle);
  struct shmem_handle *handle = (struct shmem_handle *)_handle;
  struct extent *inc = get_incoming_data_extent(handle);
  if (!inc)
    return NULL;

  struct shmem_pipe_buf* buf = malloc(sizeof(struct shmem_pipe_buf));
  buf->iov.iov_base = ((char*)handle->recv->ring) + inc->base1;
  buf->iov.iov_len = (inc->size < len ? inc->size : len);
  buf->base.bufs = &buf->iov;
  buf->base.nbufs = 1;

  /* Consume the relevant bit of buffer. */
  inc->base1 += buf->iov.iov_len;
  inc->size -= buf->iov.iov_len;

  DBGPRINT("return buffer %p\n", (void *)&buf->base);
  return &buf->base;
}

void fable_release_read_buf_shmem_pipe(struct fable_handle* _handle, struct fable_buf* fbuf)
{
  DBGPRINT("release read buf %p on %p\n", (void *)fbuf, (void *)_handle);
  struct shmem_handle *handle = (struct shmem_handle *)_handle;

  queue_extent_release(handle,
		       (unsigned long)fbuf->bufs[0].iov_base - (unsigned long)handle->recv->ring,
		       fbuf->bufs[0].iov_len);

  free(fbuf);
}

/* Return 1 if we actually managed to reclaim something, 0 if we
   didn't, and -1 if there was an error. */
static int wait_for_returned_buffers(struct shmem_handle *sp)
{
  int s;
  int done_something;

  DBGPRINT("Waiting for returned buffers on %p\n", (void *)sp);
  assert(sp->send->nr_alloc_nodes <= MAX_ALLOC_NODES);
  s = fill_incoming_buffers(sp);
  if (s <= 0)
    return s;

  done_something = 0;
  while (sp->rx_buf_cons < sp->rx_buf_prod) {
    struct extent *e = (struct extent *)(sp->rx_buf + sp->rx_buf_cons);
    if (e->size == 0) {
      sp->rx_buf_cons += sizeof(struct extent);
    } else if (e->type == EXTENT_TYPE_RELEASE) {
      done_something = 1;
      release_shared_space(sp->send, e->base1, e->size);
      sp->rx_buf_cons += sizeof(struct extent);
    } else {
      break;
    }
  }

  if (!done_something) {
    /* Do a more aggressive scan which looks past any data blocks
       which might have gotten ahead of release blocks. */
    unsigned x;
    for (x = sp->rx_buf_cons; x < sp->rx_buf_prod; x++) {
      struct extent *e = (struct extent *)(sp->rx_buf + sp->rx_buf_cons);
      if (e->type == EXTENT_TYPE_RELEASE &&
	  e->size != 0) {
	release_shared_space(sp->send, e->base1, e->size);
	done_something = 1;
	e->size = 0;
      }
      x += sizeof(struct extent);
    }
  }

  assert(sp->send->nr_alloc_nodes <= MAX_ALLOC_NODES);

  return done_something;
}

struct fable_buf* fable_get_write_buf_shmem_pipe(struct fable_handle* handle, unsigned len)
{
  DBGPRINT("get write buf on %p\n", (void *)handle);
  struct shmem_handle *sp = (struct shmem_handle *)handle;
  unsigned long offset;
  unsigned allocated_len = len;

  assert(sp->send->nr_alloc_nodes <= MAX_ALLOC_NODES);

  while ((offset = alloc_shared_space(sp->send, &allocated_len)) == ALLOC_FAILED) {
    int wait_ret = wait_for_returned_buffers(sp);
    if(wait_ret == 0)
      errno = 0;
    if(wait_ret <= 0)
      return 0;
  }

  struct shmem_pipe_buf* buf = malloc(sizeof(struct shmem_pipe_buf));

  buf->iov.iov_base = ((char*)sp->send->ring) + offset;
  buf->iov.iov_len = allocated_len;
  buf->base.bufs = &buf->iov;
  buf->base.nbufs = 1;

  if (!any_shared_space(sp->send))
    DBGPRINT("Allocated the last scrap of send buffer space...\n");

  assert(sp->send->nr_alloc_nodes <= MAX_ALLOC_NODES);

  return &buf->base;
}

int fable_release_write_buf_shmem_pipe(struct fable_handle* handle, struct fable_buf* fbuf)
{
  struct shmem_handle *sp = (struct shmem_handle *)handle;
  unsigned x;

  DBGPRINT("release write buf on %p\n", (void*)handle);

  for (x = 0; x < fbuf->nbufs; x++)
    queue_extent_transmit(sp,
			  (unsigned long)fbuf->bufs[x].iov_base - (unsigned long)sp->send->ring,
			  fbuf->bufs[x].iov_len);
  flush_outgoing_extent_queue(sp);

  return 1;
}

// Warning: this is a little broken, as there are places where we do blocking reads or writes against the extent socket.
// The hope is this won't make much difference because the extents are unlikely to fill enough space to ever actually block.
void fable_set_nonblocking_shmem_pipe(struct fable_handle* handle)
{
  struct shmem_handle *h = (struct shmem_handle *)handle;
  setnb_fd(h->fd);
}

static int action_needs_fd(struct shmem_handle *handle, int type)
{
  if (type == FABLE_SELECT_ACCEPT)
    return 1;

  if (handle->rx_buf_prod - handle->rx_buf_cons >= sizeof(struct extent))
    return 0;

  switch (type) {
  case FABLE_SELECT_READ:
    return 1;
  case FABLE_SELECT_WRITE:
    if (any_shared_space(handle->send))
      return 0;
    else
      return 1;
  case FABLE_SELECT_ACCEPT:
  default:
    abort();
  }
}

void fable_get_select_fds_shmem_pipe(struct fable_handle* handle, int type, int* maxfd, fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout)
{
  struct shmem_handle *h = (struct shmem_handle *)handle;
  if (action_needs_fd(h, type)) {
    FD_SET(h->fd, rfds);
    if (h->fd >= *maxfd)
      *maxfd = h->fd + 1;
  } else {
    timeout->tv_sec = 0;
    timeout->tv_usec = 0;
  }
}

int fable_ready_shmem_pipe(struct fable_handle* handle, int type, fd_set* rfds, fd_set* wfds, fd_set* efds)
{
  struct shmem_handle *h = (struct shmem_handle *)handle;
  if (!action_needs_fd(h, type))
    return 1;
  else
    return FD_ISSET(h->fd, rfds);
}

static void
close_simplex(struct shmem_simplex *sp)
{
  if (!sp)
    return;

  munmap(sp->ring, sp->ring_pages * 4096);

  if (sp->simpl_id.is_send) {
    // Clean up the free area list
    struct alloc_node *n;
    for (n = sp->first_alloc; n;) {
      struct alloc_node* next = n->next;
      free(n);
      n = next;
    }
  }

  free(sp);
}

void fable_close_shmem_pipe(struct fable_handle* handle)
{
  struct shmem_handle *sh = (struct shmem_handle *)handle;

  close(sh->fd);
  close_simplex(sh->send);
  close_simplex(sh->recv);
  free(sh->name);
  free(sh);
}

// lend-read is a one shot read-into-this operation.
int fable_lend_read_buf_shmem_pipe(struct fable_handle* _handle, char* buf, unsigned len)
{
  struct shmem_handle *handle = (struct shmem_handle *)_handle;
  struct extent *inc;
  int consumed;
  unsigned base;

  inc = get_incoming_data_extent(handle);
  if (!inc) {
    if (errno == 0)
      return 0;
    else
      return -1;
  }

  assert(inc->type == EXTENT_TYPE_DATA);

  if (len >= inc->size) {
    /* Consume entire message */
    consumed = inc->size;
    base = inc->base1;
  } else {
    /* Just consume the first @len bytes of the message */
    consumed = len;
    base = inc->base1;
  }

  memcpy(buf, (const void *)((unsigned long)handle->recv->ring + base), consumed);
  queue_extent_release(handle, base, consumed);

  inc->base1 += consumed;
  inc->size -= consumed;

  return consumed;

}

// lend-write creates a stateful buffer object which is released like a normal one.
// Simply do the copy-in now and note the result in the 'written' field.
struct fable_buf* fable_lend_write_buf_shmem_pipe(struct fable_handle* handle, const char* buf, unsigned len)
{
  struct fable_buf* fbuf = fable_get_write_buf_shmem_pipe(handle, len);
  if(!fbuf) {
    DBGPRINT("fable_lend_write_buf_shmem_pipe: shared arena is full!\n");
    return 0;
  }

  memcpy(fbuf->bufs[0].iov_base, buf, fbuf->bufs[0].iov_len);
  fbuf->written = fbuf->bufs[0].iov_len;

  return fbuf;
}

struct fable_buf *fable_lend_write_bufs_shmem_pipe(struct fable_handle *handle,
						   struct iovec *iovs,
						   unsigned nr_iovs)
{
  struct shmem_handle *sp = (struct shmem_handle *)handle;
  struct fable_buf *res;
  size_t total_to_alloc;
  unsigned x;
  size_t total_allocated;
  unsigned nr_iovs_allocated;
  int input_iovs_consumed;
  int input_consumed_this_iov;

  total_to_alloc = 0;
  for (x = 0; x < nr_iovs; x++)
    total_to_alloc += iovs[x].iov_len;

  DBGPRINT("Sending %zd bytes from %d frags\n", total_to_alloc, nr_iovs);

  /* Simultaneously allocate shared arena space and repack the input
     vectors into as few arena chunks as possible. */
  res = malloc(sizeof(*res) + sizeof(struct iovec));
  res->written = 0;
  res->nbufs = 0;
  res->bufs = (struct iovec *)(res + 1);
  total_allocated = 0;
  nr_iovs_allocated = 1;
  input_iovs_consumed = 0;
  input_consumed_this_iov = 0;
  while (total_allocated < total_to_alloc) {
    unsigned this_alloc_size;
    unsigned this_alloc;
    this_alloc_size = total_to_alloc - total_allocated;
    this_alloc = alloc_shared_space(sp->send, &this_alloc_size);
    if (this_alloc == ALLOC_FAILED) {
      if (wait_for_returned_buffers(sp) != 1)
	break;
      continue;
    }
    DBGPRINT("Asked for %zd bytes, got %d at %d\n",
	   total_to_alloc - total_allocated,
	   this_alloc_size,
	   this_alloc);
    if (res->nbufs == nr_iovs_allocated) {
      nr_iovs_allocated += 6;
      res = realloc(res, sizeof(*res) + nr_iovs_allocated * sizeof(struct iovec));
      res->bufs = (struct iovec *)(res + 1);
    }
    res->bufs[res->nbufs].iov_base = (void *)((unsigned long)sp->send->ring + this_alloc);
    res->bufs[res->nbufs].iov_len = this_alloc_size;

    void *output_cursor = res->bufs[res->nbufs].iov_base;
    unsigned out_left_this_frag = this_alloc_size;
    while (out_left_this_frag != 0) {
      const void *input_cursor =
	(const void *)((unsigned long)iovs[input_iovs_consumed].iov_base +
		       input_consumed_this_iov);
      unsigned bytes_this_frag = iovs[input_iovs_consumed].iov_len - input_consumed_this_iov;
      if (bytes_this_frag > out_left_this_frag)
	bytes_this_frag = out_left_this_frag;
      DBGPRINT("memcpy(%p, %p, %d) (%d, %zd, %d)\n", output_cursor, input_cursor, bytes_this_frag,
	     input_iovs_consumed,
	     iovs[input_iovs_consumed].iov_len,
	     input_consumed_this_iov);
      memcpy(output_cursor, input_cursor, bytes_this_frag);
      output_cursor = (void *)((unsigned long)output_cursor + bytes_this_frag);
      out_left_this_frag -= bytes_this_frag;
      input_consumed_this_iov += bytes_this_frag;
      if (input_consumed_this_iov == iovs[input_iovs_consumed].iov_len) {
	input_iovs_consumed++;
	input_consumed_this_iov = 0;
      }
    }

    res->nbufs++;
    total_allocated += this_alloc_size;
  }

  return res;
}

const char *fable_handle_name_shmem_pipe(struct fable_handle* handle)
{
  struct shmem_handle *sh = (struct shmem_handle *)handle;
  if (!sh->name) {
    int r = asprintf(&sh->name,
		     "FABLE:SHMEM_PIPE:fd=%d",
		     sh->fd);
    (void)r;
  }
  return sh->name;
}

int fable_get_fd_read_shmem_pipe(struct fable_handle *handle, int *read_like)
{
  struct shmem_handle *h = (struct shmem_handle *)handle;
  *read_like = 1;
  return h->fd;
}

int fable_get_fd_write_shmem_pipe(struct fable_handle *handle, int *read_like)
{
  struct shmem_handle *c = (struct shmem_handle *)handle;
  *read_like = 1;
  return c->fd;
}

void fable_abandon_write_buf_shmem_pipe(struct fable_handle *handle, struct fable_buf *fbuf)
{
  struct shmem_simplex *sp = simplex_from_fable_handle(handle, SIMPLEX_SEND);
  unsigned long offset = ((char*)fbuf->bufs[0].iov_base) - ((char*)sp->ring);
  DBGPRINT("abandon buffer [%lx,%lx) on %p\n",
	 offset, offset + fbuf->bufs[0].iov_len, (void *)handle);
  assert(sp->nr_alloc_nodes <= MAX_ALLOC_NODES);
  release_shared_space(sp, offset, fbuf->bufs[0].iov_len);
  assert(sp->nr_alloc_nodes <= MAX_ALLOC_NODES);
  free(fbuf);
}

int fable_handle_is_readable_shmem_pipe(struct fable_handle *handle)
{
  struct shmem_handle *sp = (struct shmem_handle *)handle;
  return sp->rx_buf_prod - sp->rx_buf_cons >= sizeof(struct extent);
}

int fable_handle_is_writable_shmem_pipe(struct fable_handle *handle)
{
  struct shmem_handle *sp = (struct shmem_handle *)handle;
  if (sp->rx_buf_prod - sp->rx_buf_cons >= sizeof(struct extent) ||
      any_shared_space(sp->send))
    return 1;
  else
    return 0;
}

static void libevent_handler(int fd, short which, void *ctxt)
{
  struct fable_event_shmem_pipe *evt = ctxt;
  short events_fired;

  DBGPRINT("Event fired on %p\n", (void*)evt->handle);
  events_fired = which & ~EV_READ;
  if ( evt->events_wanted & EV_READ )
    events_fired |= EV_READ;
  if ( evt->events_wanted & EV_WRITE )
    events_fired |= EV_WRITE;
  if (!(evt->events_wanted & EV_PERSIST))
    evt->events_wanted = 0;
  evt->handler(evt->handle, events_fired, evt->ctxt);
  if ( ((evt->events_wanted & EV_READ) && fable_handle_is_readable_shmem_pipe(evt->handle)) ||
       ((evt->events_wanted & EV_WRITE) && fable_handle_is_writable_shmem_pipe(evt->handle)) )
      event_active(&evt->event, EV_READ, 1);
}
static void libevent_accept_handler(int fd, short which, void *ctxt)
{
  struct fable_event_shmem_pipe *evt = ctxt;
  DBGPRINT("Accept event fired on %p\n", (void *)evt->handle);
  evt->handler(evt->handle, which, evt->ctxt);
}

void fable_add_event_shmem_pipe(struct fable_event_shmem_pipe *evt,
				struct event_base *base,
				struct fable_handle *handle,
				short event_flags,
				void (*handler)(struct fable_handle *handle,
						short which, void *ctxt),
				void *ctxt)
{
  DBGPRINT("fable_add_event(%p, %x)\n", (void *)handle, event_flags);
  struct shmem_handle *sh = (struct shmem_handle *)handle;
  struct shmem_simplex *recv_sp = simplex_from_fable_handle(handle, SIMPLEX_RECV);
  struct shmem_simplex *send_sp = simplex_from_fable_handle(handle, SIMPLEX_SEND);

  evt->handle = handle;
  evt->handler = handler;
  evt->ctxt = ctxt;
  evt->base = base;
  evt->events_wanted = event_flags;

  if (!recv_sp && !send_sp) {
    DBGPRINT("Listen for accept on %d\n", sh->fd);
    event_set(&evt->event, sh->fd, EV_PERSIST|EV_READ,
	      libevent_accept_handler, evt);
    event_base_set(base, &evt->event);
    if (event_add(&evt->event, 0) == -1)
      abort();
    return;
  }

  /* Note that we look for EV_READ even when the user asked for an
     EV_WRITE wakeup! The FD here is the aux data socket, so we can
     read that when the other end has sent us some extents to
     release. */
  if (recv_sp || send_sp) {
    DBGPRINT("Have a receive SP %p, waiting for fd %d\n", (void *)recv_sp, sh->fd);
    event_set(&evt->event, sh->fd,
	      EV_PERSIST|EV_READ, libevent_handler, evt);
    event_base_set(base, &evt->event);
  }

  if ( ((event_flags & EV_READ) && fable_handle_is_readable_shmem_pipe(handle)) ||
       ((event_flags & EV_WRITE) && fable_handle_is_writable_shmem_pipe(handle)) ) {
    DBGPRINT("Request immediate event activation\n");
    event_active(&evt->event, EV_READ, 1);
  } else {
    DBGPRINT("Asking libevent to wake us up later.\n");
    if (event_add(&evt->event, 0) == -1)
      abort();
  }
}

void fable_event_del_shmem_pipe(struct fable_event_shmem_pipe *evt)
{
  event_del(&evt->event);
  evt->events_wanted = 0;
}

void fable_event_change_flags_shmem_pipe(struct fable_event_shmem_pipe *evt, short flags)
{
  fable_event_del_shmem_pipe(evt);
  fable_add_event_shmem_pipe(evt, evt->base, evt->handle, flags, evt->handler, evt->ctxt);
}
