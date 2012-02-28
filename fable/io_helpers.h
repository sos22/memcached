
#ifndef IO_HELPERS_H
#define IO_HELPERS_H

void read_all_fd(int fd, char* buf, int len);

void write_all_fd(int fd, const void* buf, int len, int ignore_close);

void setnb_fd(int fd);

int unix_send_fd(int sockfd, int sendfd);

int unix_recv_fd(int sockfd);

#endif
