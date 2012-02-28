# MCbenchmark Makefile
# Copyright (C) 2010 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=
ifeq ($(uname_S),SunOS)
  CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  CCLINK?= -ldl -lnsl -lsocket -lm
else
  CFLAGS?= -I.. -D_GNU_SOURCE -std=c99 $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  CCLINK?= -lm -lrt
endif
CCOPT= $(CFLAGS) $(CCLINK) $(ARCH) $(PROF)
DEBUG?= -g -rdynamic -ggdb 

PREFIX= /usr/local
INSTALL_BIN= $(PREFIX)/bin
INSTALL= cp -p

BENCHOBJ = ae.o anet.o mc-benchmark.o sds.o adlist.o zmalloc.o
BENCHOBJ += ../libevent/event.o ../libevent/log.o ../libevent/signal.o ../libevent/evutil.o ../libevent/epoll.o
BENCHOBJ += ../fable/fable_shmem_pipe.o ../fable/fable_unixdomain.o ../fable/io_helpers.o ../fable/fable_tcp.o

BENCHPRGNAME = mc-benchmark

all: mc-benchmark

mc-benchmark: $(BENCHOBJ)
	$(CC) -o $(BENCHPRGNAME) $(CCOPT) $(DEBUG) $(BENCHOBJ)

.c.o:
	$(CC) -c $(CFLAGS) $(DEBUG) $(COMPILE_TIME) $< -o $@

clean:
	rm -rf $(BENCHPRGNAME) *.o

dep:
	$(CC) -MM *.c

bench:
	./mc-benchmark

