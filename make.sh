#!/bin/bash
#
# make.sh : KV SDK buildscript 
#

set -e
SDK=libuio.a
MPDK_TARGET=x86_64-native-linuxapp-gcc
ROOT=$(readlink -f $(dirname $0))

. $ROOT/script/build.sh


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

*)
	echo "Usage: make.sh {intel|all|io|driver|sdk|app|clean|intel_clean}"
	exit 1
	;;
esac

exit 0

