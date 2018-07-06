all:
	make -C fio_plugin
	make -C mkfs
	make -C fuse
	make -C unvme_rocksdb db_bench -j 4

clean:
	make -C fio_plugin clean
	make -C mkfs clean
	make -C fuse clean
	make -C unvme_rocksdb clean

