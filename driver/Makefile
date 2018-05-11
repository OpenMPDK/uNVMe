export KV_DIR = $(shell pwd)

all:
	make -C core
	make -C test

clean:
	make -C core clean
	make -C test clean

install:
	make -C core install
	make -C test install
