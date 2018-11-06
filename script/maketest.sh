#
#!/bin/bash
#

test_setup(){

	DEV_ID=$(grep dev_id ./script/test_config.json)
	SSD_TYPE=$(grep ssd_type ./script/test_config.json)

	DEV_LINE=$(grep -n dev_id ./io/kv_sdk_sync_config.json | cut -d: -f1)
	SSD_LINE=$(grep -n ssd_type ./io/kv_sdk_sync_config.json | cut -d: -f1)
	sed -i '/dev_id/d' ./io/kv_sdk_sync_config.json
	sed -i "$DEV_LINE i\\$DEV_ID" ./io/kv_sdk_sync_config.json
	sed -i '/ssd_type/d' ./io/kv_sdk_sync_config.json
	sed -i "$SSD_LINE i\\$SSD_TYPE" ./io/kv_sdk_sync_config.json
	
	DEV_LINE=$(grep -n dev_id ./io/kv_sdk_async_config.json | cut -d: -f1)
	SSD_LINE=$(grep -n ssd_type ./io/kv_sdk_async_config.json | cut -d: -f1)
	sed -i '/dev_id/d' ./io/kv_sdk_async_config.json
	sed -i "$DEV_LINE i\\$DEV_ID" ./io/kv_sdk_async_config.json
	sed -i '/ssd_type/d' ./io/kv_sdk_async_config.json
	sed -i "$SSD_LINE i\\$SSD_TYPE" ./io/kv_sdk_async_config.json
	
	DEV_LINE=$(grep -n dev_id ./io/kv_perf_scripts/kv_perf_default_config.json | cut -d: -f1)
	SSD_LINE=$(grep -n ssd_type ./io/kv_perf_scripts/kv_perf_default_config.json | cut -d: -f1)
	sed -i '/dev_id/d' ./io/kv_perf_scripts/kv_perf_default_config.json
	sed -i "$DEV_LINE i\\$DEV_ID" ./io/kv_perf_scripts/kv_perf_default_config.json
	sed -i '/ssd_type/d' ./io/kv_perf_scripts/kv_perf_default_config.json
	sed -i "$SSD_LINE i\\$SSD_TYPE" ./io/kv_perf_scripts/kv_perf_default_config.json
	
	DEV_LINE=$(grep -n dev_id ./app/fio_plugin/unvme2_config.json | cut -d: -f1)
	SSD_LINE=$(grep -n ssd_type ./app/fio_plugin/unvme2_config.json | cut -d: -f1)
	sed -i '/dev_id/d' ./app/fio_plugin/unvme2_config.json
	sed -i "$DEV_LINE i\\$DEV_ID" ./app/fio_plugin/unvme2_config.json
	sed -i '/ssd_type/d' ./app/fio_plugin/unvme2_config.json
	sed -i "$SSD_LINE i\ $SSD_TYPE" ./app/fio_plugin/unvme2_config.json

	DEV_INFO=$(grep -n dev_id ./script/test_config.json | cut -d: -f4)
	DEV_LINE=$(grep -n filename ./app/fio_plugin/Sample_Write.fio | cut -d: -f1)
	sed -i '/filename/d' ./app/fio_plugin/Sample_Write.fio
	sed -i "$DEV_LINE i\filename=0000.$DEV_INFO.00.0" ./app/fio_plugin/Sample_Write.fio

	DEV_LINE=$(grep -n "nvme_pci_dev = " ./driver/test/udd_perf/udd_perf.c | cut -d: -f1)
	sed -i '/nvme_pci_dev = /d' ./driver/test/udd_perf/udd_perf.c
	sed -i "$DEV_LINE i\	char *nvme_pci_dev = \"0000:$DEV_INFO:00.0\";" ./driver/test/udd_perf/udd_perf.c
	
	DEV_LINE=$(grep -n "nvme_pci_dev = " ./driver/test/udd_perf_async/udd_perf_async.c | cut -d: -f1)
	sed -i '/nvme_pci_dev = /d' ./driver/test/udd_perf_async/udd_perf_async.c
	sed -i "$DEV_LINE i\	char *nvme_pci_dev = \"0000:$DEV_INFO:00.0\";" ./driver/test/udd_perf_async/udd_perf_async.c

	DEV_LINE=$(grep -n 0000: ./io/tests/sdk_iterate.c | cut -d: -f1)
	sed -i '/0000:/d' ./io/tests/sdk_iterate.c
	sed -i "$DEV_LINE i\	strcpy\(sdk_opt.dev_id[0], \"0000:$DEV_INFO:00.0\"\);" ./io/tests/sdk_iterate.c
	
	DEV_LINE=$(grep -n 0000: ./io/tests/sdk_iterate_async.c | cut -d: -f1)
	sed -i '/0000:/d' ./io/tests/sdk_iterate_async.c
	sed -i "$DEV_LINE i\	strcpy\(sdk_opt.dev_id[0], \"0000:$DEV_INFO:00.0\"\);" ./io/tests/sdk_iterate_async.c

	sed -i '4d' ./app/fio_plugin/basic_validation.sh
	TYPE_CHECK=$(grep -n ssd_type ./script/test_config.json | cut -d: -f3)
	#COMP_TYPE='"lba",'
	if [ $TYPE_CHECK = '"lba",' ]; then
		SSD_TYPE_VALUE=1
		UDD_SSD_TYPE=LBA_TYPE_SSD
		sed -i "4 i\SSD_TYPE=lba" ./app/fio_plugin/basic_validation.sh
	else
		SSD_TYPE_VALUE=0
		UDD_SSD_TYPE=KV_TYPE_SSD
		sed -i "4 i\SSD_TYPE=kv" ./app/fio_plugin/basic_validation.sh
	fi

	UDD_SSD_LINE=$(grep -n "ssd_type = " ./driver/test/udd_perf/udd_perf.c | cut -d: -f1)
	sed -i '/ssd_type = /d' ./driver/test/udd_perf/udd_perf.c
	sed -i "$UDD_SSD_LINE i\	int ssd_type = $UDD_SSD_TYPE;" ./driver/test/udd_perf/udd_perf.c
	
	UDD_SSD_LINE=$(grep -n "ssd_type = " ./driver/test/udd_perf_async/udd_perf_async.c | cut -d: -f1)
	sed -i '/ssd_type = /d' ./driver/test/udd_perf_async/udd_perf_async.c
	sed -i "$UDD_SSD_LINE i\	unsigned int ssd_type = $UDD_SSD_TYPE;" ./driver/test/udd_perf_async/udd_perf_async.c

	valgrind='valgrind --leak-check=full'

	make -C driver/test/udd_perf clean
	make -C driver/test/udd_perf
	make -C driver/test/udd_perf_async clean
	make -C driver/test/udd_perf_async
	scons -C io
}

test_udd_perf(){

	cd driver/test
	log_normal "[test] udd_perf"
	$valgrind ./udd_perf/udd_perf
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] udd_perf.. Done"
	else
		log_error "[test] udd_perf.. Error"
	fi
	sleep 1

	log_normal "[test] udd_perf_async"
	$valgrind ./udd_perf_async/udd_perf_async
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] udd_perf_async.. Done"
	else
		log_error "[test] udd_perf_async.. Error"
	fi
	sleep 1
	cd ../..
}

test_sdk_perf(){
	
	cd io
	log_normal "[test] sdk_perf"
	$valgrind ./sdk_perf 
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] sdk_perf.. Done"
	else
		log_error "[test] sdk_perf.. Error"
	fi
	sleep 1

	log_normal "[test] sdk_perf_async"
	$valgrind ./sdk_perf_async
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] sdk_perf_async.. Done"
	else
		log_error "[test] sdk_perf_async.. Error"
	fi
	sleep 1
if [ $SSD_TYPE_VALUE = 0 ];then
	log_normal "[test] sdk_iterate"
	$valgrind ./sdk_iterate 
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] sdk_iterate.. Done"
	else
		log_error "[test] sdk_iterate.. Error"
	fi
	sleep 1
	
	log_normal "[test] sdk_iterate_async"
	$valgrind ./sdk_iterate_async
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] sdk_iterate_async.. Done"
	else
		log_error "[test] sdk_iterate_async .. Error"
	fi
	sleep 1

fi
	cd ..
}

test_kv_perf(){

	cd io
	log_normal "[test] kv_perf -w(write)"
	$valgrind ./kv_perf -w
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_perf -w(write).. Done"
	else
		log_error "[test] kv_perf -w(write).. Error"
	fi
	sleep 1

	log_normal "[test] kv_perf -r(read)"
	$valgrind ./kv_perf -r
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_perf -r(read).. Done"
	else
		log_error "[test] kv_perf -r(read).. Error"
	fi
	sleep 1

	log_normal "[test] kv_perf -e(delete)"
	$valgrind ./kv_perf -e
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_perf -e(delete).. Done"
	else
		log_error "[test] kv_perf -e(delete).. Error"
	fi
	sleep 1

	log_normal "[test] kv_perf -b(blend)"
	$valgrind ./kv_perf -b
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_perf -b(blend).. Done"
	else
		log_error "[test] kv_perf -b(blend).. Error"
	fi
	sleep 1

	log_normal "[test] kv_perf -i(info)"
	$valgrind ./kv_perf -i
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_perf -i(info).. Done"
	else
		log_error "[test] kv_perf -i(info).. Error"
	fi
	sleep 1
	cd ..
}

test_fio(){

	cd app/fio_plugin
	log_normal "[test] fio"
	$valgrind fio Sample_Write.fio
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] fio.. Done"
	else
		log_error "[test] fio.. Error"
	fi
	sleep 1
	cd ../..
}

test_fio_basic(){

	cd app/fio_plugin
	log_normal "[test] fio basic_validation"
	./basic_validation.sh 0000:$DEV_INFO:00.0
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] fio basic_validation.. Done"
	else
		log_error "[test] fio basic_validation.. Error"
	fi
	sleep 1
	cd ../..
}

test_kv_nvme(){
	
	cd app/nvme_cli

	log_normal "[test] kv_nvme get-log"
	$valgrind ./kv_nvme get-log 0000:$DEV_INFO:00.0 --log-id=0xc0, --log-len=512
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_nvme get-log.. Done"
	else
		log_error "[test] kv_nvme get-log.. Error"
	fi
	sleep 1
if [ $SSD_TYPE_VALUE = 0 ];then
	log_normal "[test] kv_nvme write"
	echo abcdefghijklmnopqrstuvwxyz > value.txt 
	$valgrind ./kv_nvme write 0000:$DEV_INFO:00.0 -k keyvalue12345678 -l 16 -v value.txt -o 0 -s 4096 -a -w -t
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_nvme write.. Done"
	else
		log_error "[test] kv_nvme write.. Error"
	fi
	sleep 1

	log_normal "[test] kv_nvme read"
	$valgrind ./kv_nvme read 0000:$DEV_INFO:00.0 -k keyvalue12345678 -l 16 -s 4096 -t -w
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_nvme read.. Done"
	else
		log_error "[test] kv_nvme read.. Error"
	fi
	sleep 1

	log_normal "[test] kv_nvme exist"
	$valgrind ./kv_nvme exist 0000:$DEV_INFO:00.0 -k keyvalue12345678 -l 16 -t -w
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_nvme exist.. Done"
	else
		log_error "[test] kv_nvme exist.. Error"
	fi
	sleep 1

	log_normal "[test] kv_nvme delete"
	$valgrind ./kv_nvme delete 0000:$DEV_INFO:00.0 -k keyvalue12345678 -l 16 -t -w
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_nvme delete.. Done"
	else
		log_error "[test] kv_nvme delete.. Error"
	fi
	sleep 1
fi
	log_normal "[test] kv_nvme list"
	$valgrind ./kv_nvme list
	ret=$?
	if [ $ret = 0 ]; then
		log_normal "[test] kv_nvme list.. Done"
	else
		log_error "[test] kv_nvme list.. Error"
	fi
	sleep 1
	cd ../..
}

run_test(){
	test_setup

	log_normal "[test]"
	
	test_udd_perf
	test_sdk_perf	
	test_kv_perf
	test_fio
	test_kv_nvme
	test_fio_basic

	log_normal "[test].. All Done"

}
