#define _GNU_SOURCE

#include "../libevent/event.h"

#define FABLE_TYPE tcp
#include "fable.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <netinet/tcp.h>

#define UNUSED_PARAMETER __attribute__((unused))

struct fable_buf_tcp {

  struct fable_buf base;
  struct iovec tcp_vec;

} __attribute__((packed));

struct fable_handle_tcp {
  int fd;
  char *name;
};

struct fable_handle_listen_tcp {
  int fd;
  char *name;
};

void fable_init_tcp()
{
}

/* Possibly a bit dubious to be doing DNS lookups in here?  Might want
   to do that in a separate phase? */
struct fable_handle *fable_connect_tcp(const char* name, int UNUSED_PARAMETER direction)
{
  const char *service;
  char *hostname;
  struct addrinfo *ai;
  int e;
  int fd;
  struct fable_handle_tcp *handle;

  service = strrchr(name, ':');
  hostname = malloc(service - name + 1);
  memcpy(hostname, name, service - name);
  hostname[service - name] = 0;

  e = getaddrinfo(hostname, service + 1, NULL, &ai);
  if (e != 0) {
    printf("Error %s looking up host %s (%s, service %s)\n",
	   gai_strerror(e),
	   hostname,
	   name, service);
    free(hostname);
    errno = e; /* XXX not even slightly sane */
    return NULL;
  }
  free(hostname);

  fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
  if (fd == -1) {
    freeaddrinfo(ai);
    return 0;
  }

  e = connect(fd, ai->ai_addr, ai->ai_addrlen);
  freeaddrinfo(ai);
  if (e == -1) {
    close(fd);
    return 0;
  }

  int yes = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1)
    abort();

  handle = malloc(sizeof(*handle));
  handle->fd = fd;
  handle->name = NULL;
  fable_set_nonblocking_tcp((struct fable_handle *)handle);
  return (struct fable_handle *)handle;
}

struct fable_handle *fable_listen_tcp(const char *service)
{
  struct addrinfo hints;
  struct addrinfo *ai;
  int e;
  int listen_fd;
  struct fable_handle_listen_tcp *handle;
  const char *port_name;
  char *host_name;

  port_name = strrchr(service, ':');
  if (port_name) {
    port_name = port_name + 1;
    host_name = malloc(port_name - service);
    memcpy(host_name, service, port_name - service - 1);
    host_name[port_name - service - 1] = 0;
  } else {
    port_name = service;
    host_name = NULL;
  }

  memset(&hints, 0, sizeof(hints));
  hints.ai_flags = AI_PASSIVE;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  e = getaddrinfo(host_name, port_name, &hints, &ai);
  free(host_name);
  if (e != 0) {
    errno = e; /* XXX */
    return NULL;
  }

  listen_fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
  if(listen_fd == -1) {
    freeaddrinfo(ai);
    return NULL;
  }

  e = bind(listen_fd, ai->ai_addr, ai->ai_addrlen);
  freeaddrinfo(ai);
  if (e == -1) {
    close(listen_fd);
    return 0;
  }

  e = listen(listen_fd, 5);
  if (e == -1) {
    close(listen_fd);
    return 0;
  }

  printf("Listening on %d\n", listen_fd);
  handle = malloc(sizeof(*handle));
  handle->fd = listen_fd;
  handle->name = NULL;
  return (struct fable_handle *)handle;
}

struct fable_handle *fable_accept_tcp(struct fable_handle *_listen_handle, int UNUSED_PARAMETER direction)
{
  struct fable_handle_listen_tcp *listen_handle = (void *)_listen_handle;
  struct fable_handle_tcp *acc_handle;
  int newfd;

  newfd = accept(listen_handle->fd, NULL, NULL);
  if (newfd == -1)
    return NULL;
  acc_handle = malloc(sizeof(*acc_handle));
  acc_handle->fd = newfd;
  acc_handle->name = NULL;
  fable_set_nonblocking_tcp((struct fable_handle *)acc_handle);
  int yes = 1;
  if (setsockopt(newfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1)
    abort();
  return (struct fable_handle *)acc_handle;
}

void fable_set_nonblocking_tcp(struct fable_handle *handle)
{
  /* XXX we should really have different handle types for listening
     sockets and connected ones. */
  int fd = *(int *)handle;
  int flags = fcntl(fd, F_GETFL);
  flags |= O_NONBLOCK;
  fcntl(fd, F_SETFL, flags);
}

void fable_get_select_fds_tcp(struct fable_handle *handle, int type, int* maxfd,
			      fd_set* rfds, fd_set* wfds, fd_set UNUSED_PARAMETER * efds,
			      struct timeval UNUSED_PARAMETER * timeout)
{
  int fd;
  if (type == FABLE_SELECT_ACCEPT)
    fd = ((struct fable_handle_listen_tcp *)handle)->fd;
  else
    fd = ((struct fable_handle_tcp *)handle)->fd;
  if(type == FABLE_SELECT_ACCEPT || type == FABLE_SELECT_READ)
    FD_SET(fd, rfds);
  else
    FD_SET(fd, wfds);
  if(fd >= *maxfd)
    *maxfd = (fd + 1);

}

int fable_ready_tcp(struct fable_handle *handle, int type, fd_set* rfds,
		    fd_set* wfds, UNUSED_PARAMETER fd_set* efds)
{
  int fd;
  if (type == FABLE_SELECT_ACCEPT)
    fd = ((struct fable_handle_listen_tcp *)handle)->fd;
  else
    fd = ((struct fable_handle_tcp *)handle)->fd;
  if(type == FABLE_SELECT_ACCEPT || type == FABLE_SELECT_READ)
    return FD_ISSET(fd, rfds);
  else
    return FD_ISSET(fd, wfds);

}

struct fable_buf* fable_get_write_buf_tcp(UNUSED_PARAMETER struct fable_handle *handle,
					  unsigned len)
{
  int malloc_sz = sizeof(struct fable_buf_tcp) + len;
  if(malloc_sz > 4096) {
    len -= (malloc_sz - 4096);
    malloc_sz = 4096;
  }

  struct fable_buf_tcp* new_buf = (struct fable_buf_tcp*)malloc(malloc_sz);
  new_buf->base.bufs = &new_buf->tcp_vec;
  new_buf->base.nbufs = 1;
  new_buf->base.written = 0;
  new_buf->tcp_vec.iov_len = len;
  new_buf->tcp_vec.iov_base = (char*)&(new_buf[1]);

  return &(new_buf->base);

}

struct fable_buf* fable_lend_write_buf_tcp(UNUSED_PARAMETER struct fable_handle *handle,
					   const char* buf, unsigned len)
{
  struct fable_buf_tcp* new_buf = (struct fable_buf_tcp*)malloc(sizeof(struct fable_buf_tcp));
  new_buf->base.bufs = &new_buf->tcp_vec;
  new_buf->base.nbufs = 1;
  new_buf->base.written = 0;
  new_buf->tcp_vec.iov_base = (void*)buf;
  new_buf->tcp_vec.iov_len = len;
  
  return &(new_buf->base);

}

int fable_release_write_buf_tcp(struct fable_handle *handle, struct fable_buf* buf)
{
  int fd = ((struct fable_handle_tcp *)handle)->fd;
  int remaining_write = buf->bufs[0].iov_len - buf->written;
  int write_ret = write(fd, ((char*)buf->bufs[0].iov_base) + buf->written, remaining_write);

  // Freeing buf should be safe as it's the first member of struct fable_buf_tcp.
  if(write_ret == -1) {
    if(errno == EAGAIN || errno == EINTR) {
      errno = EAGAIN;
      return -1;
    }
    else {
      free(buf);
      return -1;
    }
  }
  else if(write_ret == 0) {
    free(buf);
    return 0;
  }
  else if(write_ret < remaining_write) {
    errno = EAGAIN;
    buf->written += write_ret;
    return -1;
  }
  else {
    free(buf);
    return 1;
  }

}

int fable_lend_read_buf_tcp(struct fable_handle *handle, char* buf, unsigned len)
{
  int fd = ((struct fable_handle_tcp *)handle)->fd;
  return read(fd, buf, len);

}

struct fable_buf* fable_get_read_buf_tcp(struct fable_handle *handle, unsigned len)
{
  int fd = ((struct fable_handle_tcp *)handle)->fd;
  int malloc_sz = sizeof(struct fable_buf_tcp) + len;
  struct fable_buf_tcp* ret;
  int this_read;

  if(malloc_sz > 4096) {
    len -= (malloc_sz - 4096);
    malloc_sz = 4096;
  }

  ret = (struct fable_buf_tcp*)malloc(malloc_sz);
  ret->base.written = 0;
  ret->base.nbufs = 1;
  ret->base.bufs = &ret->tcp_vec;

  ret->tcp_vec.iov_base = &(ret[1]);

  this_read = read(fd, ret->tcp_vec.iov_base, len);
  if(this_read <= 0) {
    free(ret);
    if(this_read == 0)
      errno = 0;
    return 0;
  }
  else if(((unsigned)this_read) < len) {
    /* XXX Why is this a good idea? */
    ret = (struct fable_buf_tcp*)realloc(ret, sizeof(struct fable_buf_tcp) + this_read);
    ret->tcp_vec.iov_base = &(ret[1]);
  }
  ret->tcp_vec.iov_len = this_read;
  
  return &(ret->base);

}

void fable_release_read_buf_tcp(UNUSED_PARAMETER struct fable_handle *handle,
				struct fable_buf* buf)
{
  free(buf); // == address of 'superstruct' fable_buf_unix

}

/* XXX this should be different for listening sockets and data
   sockets. */
void fable_close_tcp(struct fable_handle *handle)
{
  struct fable_handle_tcp *h = (struct fable_handle_tcp *)handle;
  close(h->fd);
  free(h->name);
  free(handle);
}

const char *fable_handle_name_tcp(struct fable_handle *handle)
{
  struct fable_handle_tcp *h = (struct fable_handle_tcp *)handle;
  if (!h->name) {
    int r = asprintf(&h->name, "FABLE:%d", h->fd);
    (void)r;
  }
  return h->name;
}

int fable_get_fd_read_tcp(struct fable_handle *handle, int *read)
{
  struct fable_handle_tcp *h = (struct fable_handle_tcp *)handle;
  *read = 1;
  return h->fd;
}

int fable_get_fd_write_tcp(struct fable_handle *handle, int *read)
{
  struct fable_handle_tcp *h = (struct fable_handle_tcp *)handle;
  *read = 0;
  return h->fd;
}

/* These are only supposed to return true if the handle is
   readable/writable but the underlying FD isn't.  Unix domain handles
   are such simple wrappers around FDs that that never actually
   happens. */
int fable_handle_is_readable_tcp(struct fable_handle *handle) {
  return 0;
}
int fable_handle_is_writable_tcp(struct fable_handle *handle) {
  return 0;
}

void fable_abandon_write_buf_tcp(UNUSED_PARAMETER struct fable_handle *handle,
				 struct fable_buf *buf)
{
  free(buf);
}


/* libevent integration */
static void libevent_event_handler(int fd, short which, void *ctxt)
{
  struct fable_event_tcp *evt = ctxt;
  evt->handler(evt->handle, which, evt->ctxt);
}
void fable_add_event_tcp(struct fable_event_tcp *evt,
			 struct event_base *base,
			 struct fable_handle *handle,
			 short event_flags,
			 void (*handler)(struct fable_handle *handle,
					 short which, void *ctxt),
			 void *ctxt)
{
  struct fable_handle_tcp *h = (struct fable_handle_tcp *)handle;
  evt->handle = handle;
  evt->handler = handler;
  evt->ctxt = ctxt;
  event_set(&evt->event, h->fd, event_flags, libevent_event_handler, evt);
  event_base_set(base, &evt->event);

  if (event_add(&evt->event, 0) == -1)
    abort();
}

void fable_event_del(struct fable_event_tcp *evt)
{
  event_del(&evt->event);
}

void fable_event_change_flags(struct fable_event_tcp *evt, short flags)
{
  event_del(&evt->event);
  fable_add_event(evt, evt->event.ev_base, evt->handle, flags, evt->handler, evt->ctxt);
}
