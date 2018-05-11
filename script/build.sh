#
#!/bin/bash
#

set_red(){
	echo -e "\033[31m"
}
set_green(){
	echo -e "\033[32m"
}

set_white(){
	echo -e "\033[0m"
}

log_normal(){
	set_green && echo $1 && set_white
}

log_error(){
	set_red && echo $1 && set_white
}

build_driver(){
	log_normal "[Build Driver]"
	cd driver
	make clean && make 
	ret=$?
	cd ..
	if [ $ret = 0 ]; then
		log_normal "[Build Driver].. Done"
	else
		log_error "[Build Driver].. Error"
	fi
	return $ret
}

build_io(){
	log_normal "[Build IO]"
	cd io

	if [ -f "deps/check-0.9.8" ]; then
		cd deps/check-0.9.8/
		if [ ! -f "./lib/.libs/libcompat.a" ]; then
			./configure && make -j 4 && make install
		fi
		ret=$?
		cd ../..
		if [ $ret = 0 ]; then
			log_normal "[Build IO-check].. Done"
		else
			log_error "[Build IO-check].. Error"
			return $ret
		fi
	fi

	scons -c && scons
	ret=$?
	cd ..
	if [ $ret = 0 ]; then
		log_normal "[Build IO].. Done"
	else
		log_error "[Build IO].. Error"
	fi
	return $ret
}

build_all(){
	log_normal "[Build All]"
	build_driver && build_io && build_sdk
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[Build All].. Done"
	else
		log_error "[Build All].. Error"
	fi
	return $ret
}

build_intel(){
	log_normal "[Build dpdk - $MPDK_TARGET]"
	cd driver/external/dpdk
	make install T=$MPDK_TARGET EXTRA_CFLAGS="-fPIC" DESTDIR=. -j 4
	ret=$?
	cd ../../..
	if [ $ret = 0 ]; then
		log_normal "[Build dpdk - $MPDK_TARGET].. Done"
	else
		log_error "[Build dpdk - $MPDK_TARGET].. Error"
	fi

	
	log_normal "[Build spdk]"
	cd driver/external/spdk
	make DPDK_DIR=../dpdk/$MPDK_TARGET -j 4
	ret=$?
	cd ../../..
	if [ $ret = 0 ]; then
		log_normal "[Build spdk].. Done"
	else
		log_error "[Build spdk].. Error"
	fi
	return $ret
}

build_app(){
	if [ ! -f "./bin/$SDK" ]; then
		build_all
	fi
	cd app

	log_normal "[Build App - fio_plugin]"
	cd fio_plugin
	make clean && make
	ret=$?
	cd ..
	if [ $ret = 0 ]; then
		log_normal "[Build App - fio_plugin].. Done"
	else
		log_error "[Build App - fio_plugin].. Error"
	fi

	if [ -d "nvme_cli" ]; then
		log_normal "[Build App - nvme_cli]"
		cd nvme_cli
		make clean && make
		ret=$?
		cd ..
		if [ $ret = 0 ]; then
			log_normal "[Build App - nvme_cli].. Done"
		else
			log_error "[Build App - nvme_cli].. Error"
		fi
	fi

	if [ -d "kv_rocksdb" ]; then
		log_normal "[Build App - kv_db_bench]"
		cd kv_rocksdb
		make kv_db_bench -j 4
		ret=$?
		cd ..
		if [ $ret = 0 ]; then
			log_normal "[Build App - kv_db_bench].. Done"
		else
			log_error "[Build App - kv_db_bench].. Error"
		fi
	fi

	cd ..
	return $ret
}


build_analysis(){
	log_normal "[Build analyis]"
	build_driver && build_io && build_sdk
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[Build analysis - driver/io/sdk].. Done"
	else
		log_error "[Build analysis - driver/io/sdk].. Done"
	fi

	log_normal "[Build App - fio_plugin]"
	cd app/fio_plugin
	make clean && make
	ret=$?
	cd ../..
	if [ $ret = 0 ]; then
		log_normal "[Build App - fio_plugin].. Done"
	else
		log_error "[Build App - fio_plugin].. Error"
	fi

	log_normal "[Build spdk]"
	cd driver/external/spdk/lib/nvme
	make clean && make DPDK_DIR=../../../dpdk/x86_64-native-linuxapp-gcc/ -j 4
	ret=$?
	cd ..
	if [ $ret = 0 ]; then
		log_normal "[Build spdk].. Done"
	else
		log_error "[Build spdk].. Error"
	fi
	cd ../../../../..

	log_normal "[Build analyis] - Done"
	return $ret
}

build_sdk(){
	log_normal "[Build $SDK]"
	libudd=libkvnvmedd.a
	libio=libkvio.a
	rm -rf tmp bin/$SDK && mkdir -p bin tmp && cp driver/core/$libudd io/$libio tmp
	cd tmp
	ar -x $libudd
	ar -x $libio
	ar -r $SDK *.o 2>/dev/null
	ret=$?
	mv $SDK ../bin/
	rm -rf *.o *.a
	cd ..
	if [ $ret = 0 ]; then
		log_normal "[Build $SDK].. Done"
	else
		log_error "[Build $SDK].. Error"
	fi
	rm -rf tmp
	return $ret
}

clean(){
	log_normal "[Clean driver / io / sdk / app]"
	cd io && scons -c && cd ..
	cd driver && make clean && cd ..
	cd app && make clean && cd ..
	rm -rf bin
	log_normal "[Clean driver / io / sdk / app].. Done"
}

intel_clean(){
	log_normal "[Clean dpdk]"
	cd driver/external/dpdk
	rm -rf $MPDK_TARGET
	find . -name "*.a" | xargs rm	
	cd ../../..
	log_normal "Clean dpdk].. Done"

	log_normal "[Clean spdk]"
	cd driver/external/spdk
	make clean
	cd ../../..
	log_normal "[Clean spdk].. Done"
}

build_mpdk(){
	log_normal "[Build MPDK release]"
	mpdk_dir=mpdk_release
	prefix=uNVMe
	git archive --format=tar --prefix=$prefix/ HEAD | (rm -rf $mpdk_dir && mkdir $mpdk_dir && cd $mpdk_dir/ && tar xf -)

	cd $mpdk_dir/$prefix
	rm -rf app/kv_fatcache
	rm -rf app/kv_rocksdb
	rm -rf app/nvme_cli
	rm -rf app/external/performance/script/memtier_benchmark
	rm -rf app/external/performance
	rm -rf app/external/twemperf
	rm -rf app/external/radix_kernel
	rm -rf app/external/rocksdb_5.3.0
	rm -rf app/external/fatcache_0.1.0

	rm -rf driver/external/dpdk-17.11
	rm -rf driver/external/rocksdb
	rm -rf driver/external/rocksdb-5.3.3
	rm -rf driver/external/srandom
	rm -rf driver/external/common/MurmurHash3.c
	rm -rf driver/external/common/MurmurHash3.h

	rm -rf io/deps/check-0.9.8
	rm -rf io/src/jsmn.c
	rm -rf io/src/jsmn.h
	rm -rf io/src/common/MurmurHash3.c
	rm -rf io/src/common/MurmurHash3.h
	
	rm -rf io/tests/runner.c
	rm -rf io/tests/test_json.c
	
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[Build MPDK release].. Done"
	else
		log_error "[Build MPDK release].. Error"
	fi
}
