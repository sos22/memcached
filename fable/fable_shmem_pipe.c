/* Another kind of shared memory test.  The idea here is that we have
   a shared-memory region and a malloc-like thing for allocating from
   it, and we also have a couple of pipes.  Messages are sent by
   allocating a chunk of the shared region and then sending an extent
   through the pipe.  Once the receiver is finished with the message,
   they send another extent back through the other pipe saying that
   they're done. */
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

struct extent {
	unsigned base;
	unsigned size;
};

typedef struct {
  int is_send;
} simplex_id_t;

#define SIMPLEX_SEND ((simplex_id_t){1})
#define SIMPLEX_RECV ((simplex_id_t){0})

#define MAX_ALLOC_NODES 50

struct shmem_simplex {
	int fd;

        simplex_id_t simpl_id;
	void *ring;
        int ring_pages;
	struct alloc_node *first_alloc, *next_free_alloc, *last_freed_node;
#ifndef NDEBUG
	int nr_alloc_nodes;
#endif

  // Parent state
	unsigned char rx_buf[EXTENT_BUFFER_SIZE];
	unsigned rx_buf_prod;
	unsigned rx_buf_cons;

  // Child state
	unsigned char incoming[EXTENT_BUFFER_SIZE];
	struct extent outgoing_extents[EXTENT_BUFFER_SIZE/sizeof(struct extent)];
        unsigned incoming_bytes;
        unsigned incoming_bytes_consumed;
        unsigned nr_outgoing_extents;
	unsigned outgoing_extent_bytes;

};

struct shmem_handle {
  int listen_fd; /* or -1 if this isn't a listening connection */
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
fable_handle_from_shmem_simplexes(struct shmem_simplex *send,
				  struct shmem_simplex *recv)
{
  struct shmem_handle *sd = calloc(sizeof(*sd), 1);
  sd->listen_fd = -1;
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
  sd->listen_fd = fd;
  return (struct fable_handle *)sd;
}

static void shmem_pipe_init_send(struct shmem_simplex* new_handle) {

  new_handle->first_alloc = (struct alloc_node*)calloc(sizeof(*new_handle->first_alloc), 1);
  new_handle->first_alloc->is_free = 1;
  new_handle->first_alloc->end = ring_size;
  DBG(new_handle->nr_alloc_nodes = 1);

}

static struct shmem_simplex *
accept_simplex(int listen_fd, simplex_id_t simpl)
{
  void* new_unix_handle = fable_accept_unixdomain((struct fable_handle *)&listen_fd, FABLE_DIRECTION_DUPLEX);
  if(!new_unix_handle)
    return 0;

  int conn_fd = *((int*)new_unix_handle);
  free(new_unix_handle);

  // TODO: these are blocking operations, even if the Fable handle is nonblocking.
  int shmem_fd = unix_recv_fd(conn_fd);
  if(shmem_fd == -1) {
    int save_errno = errno;
    close(conn_fd);
    errno = save_errno;
    return 0;
  }

  int seg_pages;
  read_all_fd(conn_fd, (char*)&seg_pages, sizeof(int));

  void* ring_addr = mmap(NULL, PAGE_SIZE * seg_pages, PROT_READ|PROT_WRITE, MAP_SHARED, shmem_fd, 0);
  if(ring_addr == MAP_FAILED) {
    int save_errno = errno;
    close(shmem_fd);
    close(conn_fd);
    errno = save_errno;
    return 0;
  }

  struct shmem_simplex* new_handle = calloc(sizeof(struct shmem_simplex), 1);
  if(!new_handle) {
    close(shmem_fd);
    close(conn_fd);
    errno = ENOMEM;
    return 0;
  }

  new_handle->fd = conn_fd;
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
  send = NULL;
  recv = NULL;
  DBGPRINT("Accept on %p\n", (void *)handle);
  if (direction == FABLE_DIRECTION_DUPLEX || direction == FABLE_DIRECTION_RECEIVE) {
    recv = accept_simplex(h->listen_fd, SIMPLEX_RECV);
    DBGPRINT("Receive fd %d on %p\n", recv->fd, (void *)recv);
  }
  if (direction == FABLE_DIRECTION_DUPLEX || direction == FABLE_DIRECTION_SEND) {
    send = accept_simplex(h->listen_fd, SIMPLEX_SEND);
    DBGPRINT("Send fd %d\n", send->fd);
  }
  res = fable_handle_from_shmem_simplexes(send, recv);
  DBGPRINT("accepted %p\n", (void *)res);
  return res;
}

static struct shmem_simplex *
connect_simplex(const char *name, simplex_id_t simplex)
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
    return 0;

  shm_unlink(random_name);
  if (ftruncate(shm_fd, PAGE_SIZE * ring_pages) < 0) {
    close(shm_fd);
    return 0;
  }

  void* ring_addr = mmap(NULL, PAGE_SIZE * ring_pages, PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd, 0);

  void* unix_handle = fable_connect_unixdomain(name, FABLE_DIRECTION_DUPLEX);
  if(!unix_handle)
    return 0;

  int unix_fd = *((int*)unix_handle);
  free(unix_handle);

  // OK, send our partner the FD and the appropriate size to mmap.
  int send_ret = unix_send_fd(unix_fd, shm_fd);
  close(shm_fd);

  if(send_ret <= 0) {
    munmap(ring_addr, PAGE_SIZE * ring_pages);
    close(unix_fd);
    return 0;
  }

  write_all_fd(unix_fd, (const char*)&ring_pages, sizeof(int), 0);

  struct shmem_simplex* conn_handle = calloc(sizeof(struct shmem_simplex), 1);

  conn_handle->fd = unix_fd;
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
  send = NULL;
  recv = NULL;
  if (direction == FABLE_DIRECTION_DUPLEX || direction == FABLE_DIRECTION_SEND)
    send = connect_simplex(name, SIMPLEX_SEND);
  if (direction == FABLE_DIRECTION_DUPLEX || direction == FABLE_DIRECTION_RECEIVE)
    recv = connect_simplex(name, SIMPLEX_RECV);
  res = fable_handle_from_shmem_simplexes(send, recv);
  DBGPRINT("connected %p\n", (void *)res);
  return res;
}

struct fable_buf* fable_get_read_buf_shmem_pipe(struct fable_handle* handle, unsigned len)
{
  DBGPRINT("get read buf on %p\n", (void *)handle);
  struct shmem_simplex *sp = simplex_from_fable_handle(handle, SIMPLEX_RECV);

  while(sp->incoming_bytes_consumed - sp->incoming_bytes < sizeof(struct extent)) {

    int k = read(sp->fd, (char *)sp->incoming + sp->incoming_bytes, sizeof(sp->incoming) - sp->incoming_bytes);
    if (k == 0) {
      errno = 0;
      DBGPRINT("read failed: socket empty\n");
      return 0;
    }
    if (k < 0) {
      DBGPRINT("read failed: error %s\n", strerror(errno));
      if(errno == ECONNRESET)
	errno = 0;
      return 0;
    }
    sp->incoming_bytes += k;

  }

  struct extent *inc = (struct extent*)(sp->incoming + sp->incoming_bytes_consumed);

  struct shmem_pipe_buf* buf = malloc(sizeof(struct shmem_pipe_buf));
  buf->iov.iov_base = ((char*)sp->ring) + inc->base;
  buf->iov.iov_len = (inc->size < len ? inc->size : len);
  buf->base.bufs = &buf->iov;
  buf->base.nbufs = 1;

  DBGPRINT("return buffer %p\n", (void *)&buf->base);
  return &buf->base;
}

void fable_release_read_buf_shmem_pipe(struct fable_handle* handle, struct fable_buf* fbuf) {
  DBGPRINT("release read buf %p on %p\n", (void *)fbuf, (void *)handle);
  struct shmem_simplex *sp = simplex_from_fable_handle(handle, SIMPLEX_RECV);
  struct extent *inc = (struct extent*)(sp->incoming + sp->incoming_bytes_consumed);
  assert(((char*)sp->ring) + inc->base == fbuf->bufs[0].iov_base);

  struct extent tosend;
  // Was the extent fully consumed by this read?
  if(fbuf->bufs[0].iov_len == inc->size) {

    tosend = *inc;

    // Dismiss this incoming extent
    sp->incoming_bytes_consumed += sizeof(struct extent);

    if(sp->incoming_bytes_consumed - sp->incoming_bytes < sizeof(struct extent)) {
      DBGPRINT("Shuffle incoming queue down.\n");
      memmove(sp->incoming, sp->incoming + sp->incoming_bytes_consumed, sp->incoming_bytes - sp->incoming_bytes_consumed);
      sp->incoming_bytes -= sp->incoming_bytes_consumed;
      sp->incoming_bytes_consumed = 0;
    }

  }
  // Otherwise, divide the extent in two; queue one and keep the other.
  else {
    
    tosend.base = inc->base;
    tosend.size = fbuf->bufs[0].iov_len;
    inc->base += fbuf->bufs[0].iov_len;
    inc->size -= fbuf->bufs[0].iov_len;

  }

  // Queue it for transmission back to the writer
  struct extent *out;
  out = &sp->outgoing_extents[sp->nr_outgoing_extents-1];
  /* Try to reuse previous outgoing extent */
  if (sp->nr_outgoing_extents != 0 && out->base + out->size == tosend.base) {
    out->size += tosend.size;
  } else {
    sp->outgoing_extents[sp->nr_outgoing_extents] = tosend; // Struct copy
    sp->nr_outgoing_extents++;
  }
  sp->outgoing_extent_bytes += tosend.size;

  // Send the queued extents, if the queue is big enough

  // TODO: blocking operations regardless of nonblocking status.
  if (sp->outgoing_extent_bytes > ring_size / 8) {
    write_all_fd(sp->fd, (char*)sp->outgoing_extents, sp->nr_outgoing_extents * sizeof(struct extent), 1 /* Allow writing to closed socket */);
    sp->nr_outgoing_extents = 0;
    sp->outgoing_extent_bytes = 0;
  }

  free(fbuf);

}

// Return 1 for success, 0 for remote closed, -1 for error including EAGAIN
static int wait_for_returned_buffers(struct shmem_simplex *sp)
{
	int r;
	int s;
	static int total_read;

	DBGPRINT("Waiting for returned buffers on %p\n", (void *)sp);
	assert(sp->nr_alloc_nodes <= MAX_ALLOC_NODES);
	s = read(sp->fd, sp->rx_buf + sp->rx_buf_prod, sizeof(sp->rx_buf) - sp->rx_buf_prod);
	if (s <= 0) {
	  if(errno == ECONNRESET)
	    errno = 0;
	  return s;
	}
	total_read += s;
	sp->rx_buf_prod += s;
	for (r = 0; r < sp->rx_buf_prod / sizeof(struct extent); r++) {
		struct extent *e = &((struct extent *)sp->rx_buf)[r];
		release_shared_space(sp, e->base, e->size);
	}
	if (sp->rx_buf_prod != r * sizeof(struct extent))
		memmove(sp->rx_buf,
			sp->rx_buf + sp->rx_buf_prod - (sp->rx_buf_prod % sizeof(struct extent)),
			sp->rx_buf_prod % sizeof(struct extent));
	sp->rx_buf_prod %= sizeof(struct extent);
	assert(sp->nr_alloc_nodes <= MAX_ALLOC_NODES);
	return 1;
}

struct fable_buf* fable_get_write_buf_shmem_pipe(struct fable_handle* handle, unsigned len)
{
  DBGPRINT("get write buf on %p\n", (void *)handle);
  struct shmem_simplex *sp = simplex_from_fable_handle(handle, SIMPLEX_SEND);
  unsigned long offset;
  unsigned allocated_len = len;

  assert(sp->nr_alloc_nodes <= MAX_ALLOC_NODES);

  while ((offset = alloc_shared_space(sp, &allocated_len)) == ALLOC_FAILED) {
    int wait_ret = wait_for_returned_buffers(sp);
    if(wait_ret == 0)
      errno = 0;
    if(wait_ret <= 0)
      return 0;
  }

  struct shmem_pipe_buf* buf = malloc(sizeof(struct shmem_pipe_buf));

  buf->iov.iov_base = ((char*)sp->ring) + offset;
  buf->iov.iov_len = allocated_len;
  buf->base.bufs = &buf->iov;
  buf->base.nbufs = 1;

  if (!any_shared_space(sp))
    DBGPRINT("Allocated the last scrap of send buffer space...\n");

  assert(sp->nr_alloc_nodes <= MAX_ALLOC_NODES);

  return &buf->base;
}

int fable_release_write_buf_shmem_pipe(struct fable_handle* handle, struct fable_buf* fbuf)
{
  DBGPRINT("release write buf on %p\n", (void*)handle);
  struct shmem_simplex *sp = simplex_from_fable_handle(handle, SIMPLEX_SEND);
  struct extent ext;

  unsigned long offset = ((char*)fbuf->bufs[0].iov_base) - ((char*)sp->ring);
  ext.base = offset;
  ext.size = fbuf->bufs[0].iov_len;

  // TODO: Blocking ops in nonblocking context
  write_all_fd(sp->fd, (char*)&ext, sizeof(ext), 1 /* Allow writing to closed socket */);

  return 1;
}

// Warning: this is a little broken, as there are places where we do blocking reads or writes against the extent socket.
// The hope is this won't make much difference because the extents are unlikely to fill enough space to ever actually block.
void fable_set_nonblocking_shmem_pipe(struct fable_handle* handle)
{
  struct shmem_simplex *sp;

  sp = simplex_from_fable_handle(handle, SIMPLEX_RECV);
  if (sp)
    setnb_fd(sp->fd);

  sp = simplex_from_fable_handle(handle, SIMPLEX_SEND);
  if (sp)
    setnb_fd(sp->fd);
}

static int action_needs_fd(struct fable_handle *handle, int type, int *fd)
{
  struct shmem_simplex *sp;
  int _fd;
  if (!fd)
    fd = &_fd;
  switch (type) {
  case FABLE_SELECT_READ:
    sp = simplex_from_fable_handle(handle, SIMPLEX_RECV);
    if (sp->incoming_bytes_consumed - sp->incoming_bytes < sizeof(struct extent)) {
      *fd = sp->fd;
      return 1;
    } else {
      return 0;
    }
  case FABLE_SELECT_WRITE:
    sp = simplex_from_fable_handle(handle, SIMPLEX_SEND);
    if (any_shared_space(sp)) {
      return 0;
    } else {
      *fd = sp->fd;
      return 1;
    }
  case FABLE_SELECT_ACCEPT:
    *fd = ((struct shmem_handle *)handle)->listen_fd;
    return 1;
  default:
    abort();
  }
}

void fable_get_select_fds_shmem_pipe(struct fable_handle* handle, int type, int* maxfd, fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout)
{
  int fd;
  if (action_needs_fd(handle, type, &fd)) {
    FD_SET(fd, rfds);
    if(fd >= *maxfd)
      *maxfd = fd + 1;
  } else {
    timeout->tv_sec = 0;
    timeout->tv_usec = 0;
  }
}

int fable_ready_shmem_pipe(struct fable_handle* handle, int type, fd_set* rfds, fd_set* wfds, fd_set* efds)
{
  int fd;
  if (!action_needs_fd(handle, type, &fd))
    return 1;
  else
    return FD_ISSET(fd, rfds);
}

static void
close_simplex(struct shmem_simplex *sp)
{
  if (!sp)
    return;

  close(sp->fd);

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

  close_simplex(sh->send);
  close_simplex(sh->recv);
  if (sh->listen_fd >= 0)
    close(sh->listen_fd);
  free(sh->name);
  free(sh);
}

// lend-read is a one shot read-into-this operation.
int fable_lend_read_buf_shmem_pipe(struct fable_handle* handle, char* buf, unsigned len) {

  struct fable_buf* fbuf = fable_get_read_buf_shmem_pipe(handle, len);
  if(!fbuf) {
    DBGPRINT("Failed to lend read buf; %s\n", strerror(errno));
    if(errno == 0)
      return 0;
    else
      return -1;
  }

  memcpy(buf, fbuf->bufs[0].iov_base, fbuf->bufs[0].iov_len);
  unsigned ret = fbuf->bufs[0].iov_len;

  fable_release_read_buf_shmem_pipe(handle, fbuf);

  return ret;

}

// lend-write creates a stateful buffer object which is released like a normal one.
// Simply do the copy-in now and note the result in the 'written' field.
struct fable_buf* fable_lend_write_buf_shmem_pipe(struct fable_handle* handle, const char* buf, unsigned len) {

  struct fable_buf* fbuf = fable_get_write_buf_shmem_pipe(handle, len);
  if(!fbuf) {
    DBGPRINT("fable_lend_write_buf_shmem_pipe: shared arena is full!\n");
    return 0;
  }

  memcpy(fbuf->bufs[0].iov_base, buf, fbuf->bufs[0].iov_len);
  fbuf->written = fbuf->bufs[0].iov_len;

  return fbuf;

}

const char *fable_handle_name_shmem_pipe(struct fable_handle* handle)
{
  struct shmem_handle *sh = (struct shmem_handle *)handle;
  if (!sh->name) {
    int r = asprintf(&sh->name,
		     "FABLE:SHMEM_PIPE:listen=%d,send=%d,recv=%d",
		     sh->listen_fd,
		     sh->send ? sh->send->fd : -1,
		     sh->recv ? sh->recv->fd : -1);
    (void)r;
  }
  return sh->name;
}

int fable_get_fd_read_shmem_pipe(struct fable_handle *handle, int *read_like)
{
  struct shmem_simplex *c = simplex_from_fable_handle(handle, SIMPLEX_RECV);
  *read_like = 1;
  return c->fd;
}

int fable_get_fd_write_shmem_pipe(struct fable_handle *handle, int *read_like)
{
  struct shmem_simplex *c = simplex_from_fable_handle(handle, SIMPLEX_SEND);
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
  struct shmem_simplex *sp = simplex_from_fable_handle(handle, SIMPLEX_RECV);
  return sp->incoming_bytes_consumed - sp->incoming_bytes >= sizeof(struct extent);
}

int fable_handle_is_writable_shmem_pipe(struct fable_handle *handle)
{
  struct shmem_simplex *sp = simplex_from_fable_handle(handle, SIMPLEX_SEND);
  if (any_shared_space(sp))
    return 1;
  else
    return 0;
}

static void libevent_recv_handler(int fd, short which, void *ctxt)
{
  /* XXX should maybe re-insert the event if the handler doesn't clear
     it completely? */
  struct fable_event *evt = ctxt;
  DBGPRINT("Receive event fired on %p\n", (void*)evt->handle);
  evt->handler(evt->handle, which, evt->ctxt);
}
static void libevent_send_handler(int fd, short which, void *ctxt)
{
  /* XXX should maybe re-insert the event if the handler doesn't clear
     it completely? */
  struct fable_event *evt = ctxt;
  struct shmem_simplex *sp = simplex_from_fable_handle(evt->handle, SIMPLEX_SEND);
  DBGPRINT("Send event fired on %p\n", (void*)evt->handle);
  wait_for_returned_buffers(sp);
  if (any_shared_space(sp))
    evt->handler(evt->handle, EV_WRITE | (which & ~EV_READ), evt->ctxt);
}
static void libevent_accept_handler(int fd, short which, void *ctxt)
{
  struct fable_event *evt = ctxt;
  DBGPRINT("Accept event fired on %p\n", (void *)evt->handle);
  evt->handler(evt->handle, which, evt->ctxt);
}

void fable_add_event(struct fable_event *evt,
		     struct event_base *base,
		     struct fable_handle *handle,
		     short event_flags,
		     void (*handler)(struct fable_handle *handle,
				     short which, void *ctxt),
		     void *ctxt)
{
  DBGPRINT("fable_add_event(%p, %x)\n", (void *)handle, event_flags);
  struct shmem_simplex *recv_sp = simplex_from_fable_handle(handle, SIMPLEX_RECV);
  struct shmem_simplex *send_sp = simplex_from_fable_handle(handle, SIMPLEX_SEND);

  evt->handle = handle;
  evt->handler = handler;
  evt->ctxt = ctxt;
  evt->base = base;

  if (!recv_sp && !send_sp) {
    struct shmem_handle *sh = (struct shmem_handle *)handle;
    assert(sh->listen_fd >= 0);
    event_set(&evt->recv_event, sh->listen_fd, EV_PERSIST|EV_READ,
	      libevent_accept_handler, evt);
    event_base_set(base, &evt->recv_event);
    if (event_add(&evt->recv_event, 0) == -1)
      abort();
    return;
  }

  if (recv_sp) {
    DBGPRINT("Have a receive SP %p, waiting for fd %d\n", (void *)recv_sp, recv_sp->fd);
    event_set(&evt->recv_event, recv_sp->fd,
	      EV_PERSIST|EV_READ, libevent_recv_handler, evt);
    event_base_set(base, &evt->recv_event);
  }

  /* Note that we look for EV_READ even on the send channel! The FD
     here is the aux data socket, so we can read that when the other
     end has sent us something. */
  if (send_sp) {
    event_set(&evt->send_event, send_sp->fd,
	      EV_PERSIST|EV_READ, libevent_send_handler, evt);
    event_base_set(base, &evt->send_event);
  }

  if (event_flags & EV_READ) {
    DBGPRINT("Want read events, have %d consumed of %d incoming\n",
	     recv_sp->incoming_bytes_consumed, recv_sp->incoming_bytes);
    if (fable_handle_is_readable(handle)) {
      DBGPRINT("Request immediate event activation\n");
      event_active(&evt->recv_event, EV_READ, 1);
    } else {
      DBGPRINT("Asking libevent to wake us up later.\n");
      if (event_add(&evt->recv_event, 0) == -1)
	abort();
    }
  }

  if (event_flags & EV_WRITE) {
    DBGPRINT("Want write events, have space: %d\n", any_shared_space(send_sp));
    if (fable_handle_is_writable(handle)) {
      event_active(&evt->send_event, EV_READ, 1);
    } else {
      if (event_add(&evt->send_event, 0) == -1)
	abort();
    }
  }
}

void fable_event_del(struct fable_event *evt)
{
  if (simplex_from_fable_handle(evt->handle, SIMPLEX_RECV))
    event_del(&evt->recv_event);
  if (simplex_from_fable_handle(evt->handle, SIMPLEX_SEND))
    event_del(&evt->send_event);
}

void fable_event_change_flags(struct fable_event *evt, short flags)
{
  fable_event_del(evt);
  fable_add_event(evt, evt->base, evt->handle, flags, evt->handler, evt->ctxt);
}
