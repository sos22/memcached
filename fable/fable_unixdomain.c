#define _GNU_SOURCE

#include <sys/types.h>

#include <event.h>

#include "io_helpers.h"
#include "fable.h"

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/stat.h>
#include <sys/socket.h>
#include <linux/un.h> // For UNIX_PATH_MAX

char coord_dir[] = "/tmp/phoenix_coord_ABCDEF";
char fable_unix_buf[4096];

#define MAX_UNIX_BUF (4096 - (sizeof(struct fable_buf) + sizeof(struct iovec)))

struct fable_buf_unix {

  struct fable_buf base;
  struct iovec unix_vec;

} __attribute__((packed));

void fable_init_unixdomain() {
#if 0
  if(!mkdtemp(coord_dir)) {
    fprintf(stderr, "Couldn't create a coordination directory: %s\n", strerror(errno));
    exit(1);
  }
#endif
  mkdir(coord_dir, 0700);
}

// Ignore direction, all Unix sockets are duplex
struct fable_handle *fable_connect_unixdomain(const char* name, int direction) {

  struct sockaddr_un addr;
  addr.sun_family = AF_UNIX;
  int ret = snprintf(addr.sun_path, UNIX_PATH_MAX, "%s/%s", coord_dir, name);
  if(ret >= UNIX_PATH_MAX) {
    errno = ENAMETOOLONG;
    return 0;
  }
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if(fd == -1)
    return 0;

  ret = connect(fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_un));
  if(ret == -1) {
    close(fd);
    return 0;
  }

  int* handle = (int*)malloc(sizeof(int));
  *handle = fd;
  return (struct fable_handle *)handle;

}

struct fable_handle* fable_listen_unixdomain(const char* name) {

  int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if(listen_fd == -1)
    return 0;

  struct sockaddr_un addr;
  addr.sun_family = AF_UNIX;
  int ret = snprintf(addr.sun_path, UNIX_PATH_MAX, "%s/%s", coord_dir, name);
  if(ret >= 256) {
    errno = ENAMETOOLONG;
    return 0;
  }

  ret = bind(listen_fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_un));
  if(ret == -1)
    return 0;

  ret = listen(listen_fd, 5);
  if(ret == -1)
    return 0;

  int* handle = (int*)malloc(sizeof(int));
  *handle = listen_fd;
  return (struct fable_handle *)handle;

}

// Ignore direction, all Unix sockets are duplex
struct fable_handle* fable_accept_unixdomain(struct fable_handle* listen_handle, int direction) {

  struct sockaddr_un otherend;
  socklen_t otherend_len = sizeof(otherend);
  int newfd = accept(*((int*)listen_handle), (struct sockaddr*)&otherend, &otherend_len);
  if(newfd != -1) {
    int* handle = (int*)malloc(sizeof(int));
    *handle = newfd;
    return (struct fable_handle *)handle;
  }
  else
    return 0;

}

void fable_set_nonblocking_unixdomain(struct fable_handle* handle) {

  setnb_fd(*((int*)handle));

}

void fable_get_select_fds_unixdomain(struct fable_handle* handle, int type, int* maxfd, fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout) {

  int fd = *((int*)handle);
  if(type == FABLE_SELECT_ACCEPT || type == FABLE_SELECT_READ)
    FD_SET(fd, rfds);
  else
    FD_SET(fd, wfds);
  if(fd >= *maxfd)
    *maxfd = (fd + 1);

}

int fable_ready_unixdomain(struct fable_handle* handle, int type, fd_set* rfds, fd_set* wfds, fd_set* efds) {

  int fd = *((int*)handle);
  if(type == FABLE_SELECT_ACCEPT || type == FABLE_SELECT_READ)
    return FD_ISSET(fd, rfds);
  else
    return FD_ISSET(fd, wfds);

}

struct fable_buf* fable_get_write_buf_unixdomain(struct fable_handle* handle, unsigned len) {

  int malloc_sz = sizeof(struct fable_buf_unix) + len;
  if(malloc_sz > 4096) {
    len -= (malloc_sz - 4096);
    malloc_sz = 4096;
  }
  
  struct fable_buf_unix* new_buf = (struct fable_buf_unix*)malloc(malloc_sz);
  new_buf->base.bufs = &new_buf->unix_vec;
  new_buf->base.nbufs = 1;
  new_buf->base.written = 0;
  new_buf->unix_vec.iov_len = len;
  new_buf->unix_vec.iov_base = (char*)&(new_buf[1]);

  return &(new_buf->base);

}

struct fable_buf* fable_lend_write_buf_unixdomain(struct fable_handle* handle, const char* buf, unsigned len) {

  struct fable_buf_unix* new_buf = (struct fable_buf_unix*)malloc(sizeof(struct fable_buf_unix));
  new_buf->base.bufs = &new_buf->unix_vec;
  new_buf->base.nbufs = 1;
  new_buf->base.written = 0;
  new_buf->unix_vec.iov_base = (void*)buf;
  new_buf->unix_vec.iov_len = len;
  
  return &(new_buf->base);

}

struct fable_buf* fable_lend_write_bufs_unixdomain(struct fable_handle *handle,
						   struct iovec *iovs,
						   unsigned nr_iovs)
{
  struct fable_buf_unix* new_buf = (struct fable_buf_unix*)malloc(sizeof(struct fable_buf_unix));
  new_buf->base.bufs = iovs;
  new_buf->base.nbufs = nr_iovs;
  new_buf->base.written = 0;

  return &(new_buf->base);
}

int fable_release_write_buf_unixdomain(struct fable_handle *handle, struct fable_buf* buf)
{
  int fd = *(int *)handle;
  int nr_iovecs_complete;
  size_t written;
  size_t unwritten;
  ssize_t sendmsg_res;
  unsigned done_this_iov;
  unsigned x;
  struct msghdr mh;

  if (buf->nbufs == 1) {
    unwritten = buf->bufs[0].iov_len - buf->written;
    sendmsg_res = write(fd,
			(void *)((unsigned long)buf->bufs[0].iov_base + buf->written),
			unwritten);
  } else {
    written = 0;
    nr_iovecs_complete = 0;
    while (1) {
      assert(nr_iovecs_complete < buf->nbufs);
      if (written + buf->bufs[nr_iovecs_complete].iov_len > buf->written) {
	done_this_iov = written - buf->written;
	buf->bufs[nr_iovecs_complete].iov_base =
	  (void *)((unsigned long)buf->bufs[nr_iovecs_complete].iov_base + done_this_iov);
	buf->bufs[nr_iovecs_complete].iov_len -= done_this_iov;
	break;
      }
      written += buf->bufs[nr_iovecs_complete].iov_len;
      nr_iovecs_complete++;
    }

    unwritten = 0;
    for (x = nr_iovecs_complete; x < buf->nbufs; x++)
      unwritten += buf->bufs[x].iov_len;

    memset(&mh, 0, sizeof(mh));
    mh.msg_iov = buf->bufs + nr_iovecs_complete;
    mh.msg_iovlen = buf->nbufs - nr_iovecs_complete;
    sendmsg_res = sendmsg(fd,
			  &mh,
			  0);
    buf->bufs[nr_iovecs_complete].iov_base =
      (void *)((unsigned long)buf->bufs[nr_iovecs_complete].iov_base - done_this_iov);
    buf->bufs[nr_iovecs_complete].iov_len += done_this_iov;
  }

  /* The return cases:

     return 1 -> completely transmitted the buffer and freed it out
     return 0 -> got EOF without transmitting anything, freed buffer
     return -1, errno = EAGAIN -> temporary lack of buffer space,
                                  part of buffer transmitted,
				  buffer *not* freed.
     return -1, errno != EAGAIN -> hard error, nothing transmitted,
                                   buffer freed.
  */
  if(sendmsg_res == -1) {
    if(errno == EAGAIN || errno == EINTR) {
      errno = EAGAIN;
      return -1;
    }
    else {
      free(buf);
      return -1;
    }
  }
  if (sendmsg_res == 0) {
    free(buf);
    return 0;
  }
  if (sendmsg_res < unwritten) {
    errno = EAGAIN;
    buf->written += sendmsg_res;
    return -1;
  }

  free(buf);
  return 1;
}

void fable_abandon_write_buf_unixdomain(struct fable_handle *handle, struct fable_buf *buf) {
  free(buf);
}

int fable_lend_read_buf_unixdomain(struct fable_handle* handle, char* buf, unsigned len) {

  int fd = *((int*)handle);
  return read(fd, buf, len);

}

struct fable_buf* fable_get_read_buf_unixdomain(struct fable_handle* handle, unsigned len) {

  int malloc_sz = sizeof(struct fable_buf_unix) + len;
  if(malloc_sz > 4096) {
    len -= (malloc_sz - 4096);
    malloc_sz = 4096;
  }

  struct fable_buf_unix* ret = (struct fable_buf_unix*)malloc(malloc_sz);
  ret->base.written = 0;
  ret->base.nbufs = 1;
  ret->base.bufs = &ret->unix_vec;

  ret->unix_vec.iov_base = &(ret[1]);
  
  int fd = *((int*)handle);
  int this_read = read(fd, ret->unix_vec.iov_base, len);
  if(this_read <= 0) {
    free(ret);
    if(this_read == 0)
      errno = 0;
    return 0;
  }
  else if(((unsigned)this_read) < len) {
    ret = (struct fable_buf_unix*)realloc(ret, sizeof(struct fable_buf_unix) + this_read);
    ret->unix_vec.iov_base = &(ret[1]);
  }
  ret->unix_vec.iov_len = this_read;
  
  return &(ret->base);

}

void fable_release_read_buf_unixdomain(struct fable_handle* handle, struct fable_buf* buf) {

  free(buf); // == address of 'superstruct' fable_buf_unix

}

void fable_close_unixdomain(struct fable_handle* handle) {

  close(*((int*)handle));
  free(handle);

}

const char *fable_handle_name_unixdomain(struct fable_handle *handle)
{
  return "unixdomain";
}

int fable_get_fd_read_unixdomain(struct fable_handle *handle, int *read) {
  *read = 1;
  return *(int *)handle;
}

int fable_get_fd_write_unixdomain(struct fable_handle *handle, int *read) {
  *read = 0;
  return *(int *)handle;
}

/* These are only supposed to return true if the handle is
   readable/writable but the underlying FD isn't.  Unix domain handles
   are such simple wrappers around FDs that that never actually
   happens. */
int fable_handle_is_readable_unixdomain(struct fable_handle *handle) {
  return 0;
}
int fable_handle_is_writable_unixdomain(struct fable_handle *handle) {
  return 0;
}

static void libevent_event_handler(int fd, short which, void *ctxt)
{
  struct fable_event_unixdomain *evt = ctxt;
  evt->handler(evt->handle, which, evt->ctxt);
}

void fable_add_event_unixdomain(struct fable_event_unixdomain *evt,
				struct event_base *base,
				struct fable_handle *handle,
				short event_flags,
				void (*handler)(struct fable_handle *handle,
						short which,
						void *ctxt),
				void *ctxt)
{
  evt->handle = handle;
  evt->handler = handler;
  evt->ctxt = ctxt;
  event_set(&evt->event, *(int *)handle, event_flags,
	    libevent_event_handler, evt);
  event_base_set(base, &evt->event);
  if (event_add(&evt->event, 0) == -1)
    abort();
}

void fable_event_del_unixdomain(struct fable_event_unixdomain *evt)
{
  event_del(&evt->event);
}

void fable_event_change_flags_unixdomain(struct fable_event_unixdomain *evt, short flags)
{
  fable_event_del_unixdomain(evt);
  fable_add_event_unixdomain(evt, evt->event.ev_base, evt->handle, flags, evt->handler, evt->ctxt);
}
