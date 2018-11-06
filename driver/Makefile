export KV_DIR = $(shell pwd)/../

include $(KV_DIR)/mk/unvme.mk

all:
	make -C core -f $(MAKEFILE)
	make -C test

clean:
	make -C core clean
	make -C test clean

install:
	make -C core install
	make -C test install
