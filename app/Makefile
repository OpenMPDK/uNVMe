all:
	make -C fio_plugin
	make -C nvme_cli
	make -C kv_rocksdb kv_db_bench -j 4

clean:
	make -C fio_plugin clean
	make -C nvme_cli clean
	make -C kv_rocksdb clean

