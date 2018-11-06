#!/bin/bash

#### JSON DESCRIPTION
SSD_TYPE=lba

#### FIO DESCRIPTION
BLOCKSIZE=4k
TIME_BASED=0
RAMP_TIME=0
RUNTIME=0
SIZE=10G

#### DO NOT MODIFY BELOW
IOENGINE=./unvme2_fio_plugin
JSON_CONF=./basic_test_config.json
FIO_JOB_FILE=./basic_test_config.fio
FIO=`ls | grep fio-`

# make_json(dev_id, core_mask(, dev_id2, core_mask2))
make_json()
{
	if [ -f $JSON_CONF ]; then
		rm -f $JSON_CONF
	fi

	echo "{" > $JSON_CONF
	echo \"ssd_type\":\"$SSD_TYPE\", >> $JSON_CONF
	echo \"device_description\":"[" >> $JSON_CONF
	echo "{" >> $JSON_CONF
	echo \"dev_id\":\"$1\", >> $JSON_CONF
	echo \"core_mask\":\"$2\" >> $JSON_CONF
	if [ ! -z $3 ]; then
		echo "}," >> $JSON_CONF
		echo "{" >> $JSON_CONF
		echo \"dev_id\":\"$3\", >> $JSON_CONF
		echo \"core_mask\":\"$4\" >> $JSON_CONF
	fi
	echo "}" >> $JSON_CONF
	echo "]" >> $JSON_CONF
	echo "}" >> $JSON_CONF
}

# make_fio_description(numjobs, device_bdf(, device_bdf2))
make_fio_description()
{
        if [ -f $FIO_JOB_FILE ]; then
                rm -f $FIO_JOB_FILE
        fi

	echo "[global]" > $FIO_JOB_FILE
	echo "ioengine=$IOENGINE" >> $FIO_JOB_FILE
	echo "json_path=$JSON_CONF" >> $FIO_JOB_FILE
	echo "size=$SIZE" >> $FIO_JOB_FILE
	echo "time_based=$TIME_BASED" >> $FIO_JOB_FILE
	echo "ramp_time=$RAMP_TIME" >> $FIO_JOB_FILE
	echo "runtime=$RUNTIME" >> $FIO_JOB_FILE
	echo "blocksize=$BLOCKSIZE" >> $FIO_JOB_FILE
	echo "thread=1" >> $FIO_JOB_FILE
	echo "direct=1" >> $FIO_JOB_FILE
	echo "group_reporting=1" >> $FIO_JOB_FILE
	echo "verify=0" >> $FIO_JOB_FILE
	echo "norandommap=1" >> $FIO_JOB_FILE

	echo "[test0]" >> $FIO_JOB_FILE
	FILE_LEN=${#2}
	if [ $FILE_LEN -le 12 ]; then
		echo "filename=`echo $2 | sed -e s/:/./g`" >> $FIO_JOB_FILE
	else
		echo "filename=$2" >> $FIO_JOB_FILE
	fi
	echo "numjobs=$1" >> $FIO_JOB_FILE
	if [ ! -z $3 ]; then
		echo "[test1]" >> $FIO_JOB_FILE
		echo "filename=`echo $3 | sed -e s/:/./g`" >> $FIO_JOB_FILE
		echo "numjobs=$1" >> $FIO_JOB_FILE
	fi
}

# run_fio()
run_fio() 
{
	for OP in "write" "read"
	do
		for IODEPTH in 1 4 16 64 256
		do 
			echo "[OP: $OP IODEPTH: $IODEPTH]"
			./$FIO --readwrite=$OP --iodepth=$IODEPTH $FIO_JOB_FILE
		done
	done
}



if [ -z $FIO ];then
	echo "Please install FIO and uNVMe FIO plugin first"
	exit 0
fi

if [ -z $1 ]; then
	echo "Please specify devices(max 2)"
	echo "e.g) ./basic_validation.sh 0000:01:00.0"
	echo "     ./basic_validation.sh 0000:01:00.0 0000:02:00.0"
	exit 0
fi

DEVICE_BDF=$1
if [ ! -z $2 ];	then
	DEVICE_BDF2=$2
fi

#======================= Single Device =========================
# Device : Job = 1 : 1
CORE_MASK=0x1
NUMJOBS=1
make_json $DEVICE_BDF $CORE_MASK
make_fio_description $NUMJOBS $DEVICE_BDF
run_fio

# 1 : N
CORE_MASK=0xF
NUMJOBS=4
make_json $DEVICE_BDF $CORE_MASK
make_fio_description $NUMJOBS $DEVICE_BDF
run_fio
#===============================================================

if [ -z $DEVICE_BDF2 ]; then
	exit 0
fi

#======================= Multiple Devices ======================
# Device : Job = N : 1
CORE_MASK=0x1
CORE_MASK2=0x1
NUMJOBS=1
FILENAME="`echo $DEVICE_BDF | sed -e s/:/./g`:`echo $DEVICE_BDF2 | sed -e s/:/./g`"
make_json $DEVICE_BDF $CORE_MASK $DEVICE_BDF2 $CORE_MASK2
make_fio_description $NUMJOBS $FILENAME
run_fio

# N : N
CORE_MASK=0x1
CORE_MASK2=0x2
NUMJOBS=1
make_json $DEVICE_BDF $CORE_MASK $DEVICE_BDF2 $CORE_MASK2
make_fio_description $NUMJOBS $DEVICE_BDF $DEVICE_BDF2 
run_fio

# N : M*N
CORE_MASK=0xF
CORE_MASK2=0xF0
NUMJOBS=4
make_json $DEVICE_BDF $CORE_MASK $DEVICE_BDF2 $CORE_MASK2
make_fio_description $NUMJOBS $DEVICE_BDF $DEVICE_BDF2 
run_fio
#===============================================================

rm -f $JSON_CONF
rm -f $FIO_JOB_FILE
