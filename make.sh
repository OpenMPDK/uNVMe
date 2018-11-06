#!/bin/bash
#
# make.sh : KV SDK buildscript 
#

set -e
SDK=libuio.a
MPDK_TARGET=x86_64-native-linuxapp-gcc

#DPDK_PATH=dpdk
#SPDK_PATH=spdk
DPDK_PATH=dpdk-18.05
SPDK_PATH=spdk-18.04.1
ROOT=$(readlink -f $(dirname $0))

. $ROOT/script/build.sh
. $ROOT/script/maketest.sh

echo `pwd`

case "$1" in
all)
	build_all
	;;
sdk)
	build_sdk
	;;
io)
	build_io
	;;
driver)
	build_driver
	;;
app)
	build_app
	;;
clean)
	clean
	;;
intel)
	build_intel
	;;
intel_clean)
	intel_clean
	;;
mpdk)
	build_mpdk
	;;
analysis)
	build_analysis
	;;
test)
	run_test
	;;

*)
	echo "Usage: make.sh {intel|all|io|driver|sdk|app|clean|intel_clean|test}"
	exit 1
	;;
esac

exit 0

