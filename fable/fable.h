
#ifndef FABLE_H
#define FABLE_H

#include <sys/select.h> // For fd_set
#include <sys/socket.h>
#include <sys/uio.h> // For struct iovec
#include <stdlib.h> // For abort()

struct fable_buf {

  struct iovec* bufs;
  int nbufs;
  int written;

} __attribute__((packed));

#define CONC2(C, D) C ## D
#define CONC(A, B) CONC2(A, B)
// Necessary because sub-macros are expanded *after* expansion and *excluding* parameters to ##. Nice.
// Also, the unix transport must *not* be named 'unix' as unix, lowercase, is #defined to 1 somewhere in libc.

#ifdef FABLE_TYPE

#define fable_init CONC(fable_init_, FABLE_TYPE)
#define fable_connect CONC(fable_connect_, FABLE_TYPE)
#define fable_listen CONC(fable_listen_, FABLE_TYPE)
#define fable_accept CONC(fable_accept_, FABLE_TYPE)
#define fable_set_nonblocking CONC(fable_set_nonblocking_, FABLE_TYPE)
#define fable_get_select_fds CONC(fable_get_select_fds_, FABLE_TYPE)
#define fable_ready CONC(fable_ready_, FABLE_TYPE)
#define fable_get_write_buf CONC(fable_get_write_buf_, FABLE_TYPE)
#define fable_lend_write_buf CONC(fable_lend_write_buf_, FABLE_TYPE)
#define fable_release_write_buf CONC(fable_release_write_buf_, FABLE_TYPE)
#define fable_abandon_write_buf CONC(fable_abandon_write_buf_, FABLE_TYPE)
#define fable_get_read_buf CONC(fable_get_read_buf_, FABLE_TYPE)
#define fable_lend_read_buf CONC(fable_lend_read_buf_, FABLE_TYPE)
#define fable_release_read_buf CONC(fable_release_read_buf_, FABLE_TYPE)
#define fable_close CONC(fable_close_, FABLE_TYPE)
#define fable_handle_name CONC(fable_handle_name_, FABLE_TYPE)
#define fable_get_fd CONC(fable_get_fd_, FABLE_TYPE)

#endif

#define FABLE_SELECT_READ 1
#define FABLE_SELECT_WRITE 2
#define FABLE_SELECT_ACCEPT 3

#define FABLE_DIRECTION_DUPLEX 1
#define FABLE_DIRECTION_SEND 2
#define FABLE_DIRECTION_RECEIVE 3

struct fable_handle;

#define fable_mk_methods(name)						\
  void fable_init_ ## name (void);					\
  struct fable_handle* fable_connect_ ## name (const char* _name,	\
					       int direction);		\
  struct fable_handle* fable_listen_ ## name (const char *interface,	\
					      const char* _name);	\
  struct fable_handle* fable_accept_ ## name (struct fable_handle *listen_handle, \
					      int direction);		\
  void fable_set_nonblocking_ ## name (struct fable_handle* handle);	\
  void fable_get_select_fds_ ## name (struct fable_handle* handle,	\
				      int type,				\
				      int* maxfd, fd_set* rfds,		\
				      fd_set* wfds, fd_set* efds,	\
				      struct timeval* timeout);		\
  int fable_ready_ ## name (struct fable_handle* handle, int type,	\
			    fd_set* rfds,				\
			    fd_set* wfds, fd_set* efds);		\
  struct fable_buf* fable_get_write_buf_ ## name (struct fable_handle *handle, \
						  unsigned len);	\
  struct fable_buf* fable_lend_write_buf_ ## name (struct fable_handle *handle,	\
						   const char* buf,	\
						   unsigned len);	\
  int fable_release_write_buf_ ## name (struct fable_handle* handle,	\
					struct fable_buf* buf);		\
  void fable_abandon_write_buf_ ## name (struct fable_handle* handle,	\
					 struct fable_buf* buf);	\
  int fable_lend_read_buf_ ## name (struct fable_handle *handle,	\
				    char* buf,				\
				    unsigned len);			\
  struct fable_buf* fable_get_read_buf_ ## name (struct fable_handle *handle, \
						 unsigned len);		\
  void fable_release_read_buf_ ## name (struct fable_handle *handle,	\
					struct fable_buf* buf);		\
  void fable_close_ ## name (struct fable_handle *handle);		\
  const char *fable_handle_name_ ## name (struct fable_handle *handle);	\
  int fable_get_fd_ ## name(struct fable_handle *handle)

fable_mk_methods(unixdomain);
fable_mk_methods(tcp);
fable_mk_methods(shmem_pipe);

#undef fable_mk_methods

struct msghdr;

#define fable_blocking_read fable_lend_read_buf

static inline ssize_t fable_blocking_write(struct fable_handle *handle, const void *buf, size_t bufsize)
{
  struct fable_buf *fbuf = fable_lend_write_buf(handle, buf, bufsize);
  int r = fable_release_write_buf(handle, fbuf);
  if (r == 0)
    return 0;
  if (r == 1)
    return bufsize;
  if (r == -1) {
    int res = fbuf->written;
    fable_abandon_write_buf(handle, fbuf);
    if (res == 0)
      return -1;
    else
      return res;
  }
  abort();
}

static inline ssize_t fable_blocking_sendmsg(struct fable_handle *handle, const struct msghdr *hdr)
{
  unsigned i;
  ssize_t res = 0;
  for (i = 0; i < hdr->msg_iovlen; i++) {
    ssize_t r = fable_blocking_write(handle, hdr->msg_iov[i].iov_base, hdr->msg_iov[i].iov_len);
    if (r < 0) {
      if (res == 0) {
	res = r;
	break;
      }
    } else {
      res += r;
    }
    if (r != (ssize_t)hdr->msg_iov[i].iov_len)
      break;
  }
  return res;
}

#endif
