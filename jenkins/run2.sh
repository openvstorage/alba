#!/bin/bash -xue

env

export WORKSPACE=$PWD
echo ${WORKSPACE}

export ALBA_ASD_PATH_T="${WORKSPACE}/tmp/alba/asd/%02i"
mkdir -p ${WORKSPACE}/tmp/alba/asd/

eval `${opam_env}`

make

ulimit -c unlimited
cat /proc/sys/kernel/core_pattern

case "$1" in
    asd_start)
        fab dev.run_test_asd_start:xml=True || true
        fab alba.smoke_test
        ;;
    *)
        echo "invalid test suite specified..."
        exit1
esac
