#!/bin/bash -xue

env | sort

export WORKSPACE=$PWD
echo ${WORKSPACE}
export DRIVER=./setup/setup.native

eval `${opam_env}`

make clean
make

find cfg/*.ini -exec sed -i "s,/tmp,${WORKSPACE}/tmp,g" {} \;

id
ulimit -n 1024
ulimit -c unlimited
cat /proc/sys/kernel/core_pattern

case "$1" in
    asd_start)
        fab dev.run_test_asd_start:xml=True || true
        fab alba.smoke_test
        ;;
    cpp)
        ./jenkins/cpp/010-build_client.sh
        export LD_LIBRARY_PATH=$WORKSPACE/cpp/lib
        fab dev.run_tests_cpp:xml=True || true
        fab alba.smoke_test
        ;;
    ocaml)
        fab dev.run_tests_ocaml:xml=True || true
        fab alba.smoke_test
        ;;
    stress)
        ${DRIVER} stress || true
        ;;
    voldrv_backend)
        ${DRIVER} voldrv_backend || true
        ;;
    voldrv_tests)
        ${DRIVER} voldrv_tests || true
        ;;
    disk_failures)
        ${DRIVER} disk_failures || true
        ;;
    compat)
        fab dev.run_tests_compat:xml=True || true
        ;;
    everything_else)
        fab dev.run_everything_else:xml=True || true
        ;;
    *)
        echo "invalid test suite specified..."
        exit1
esac
