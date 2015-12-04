#!/bin/bash -xue

env | sort

export WORKSPACE=$PWD
echo ${WORKSPACE}

eval `${opam_env}`

make

find cfg/*.ini -exec sed -i "s,/tmp,${WORKSPACE}/tmp,g" {} \;

id
ulimit -n 2048
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
        fab dev.run_tests_stress:xml=True || true
        fab alba.smoke_test
        ;;
    voldrv_backend)
        fab dev.run_tests_voldrv_backend:xml=True || true
        fab alba.smoke_test
        ;;
    voldrv_tests)
        fab dev.run_tests_voldrv_tests:xml=True || true
        fab alba.smoke_test
        ;;
    disk_failures)
        fab dev.run_tests_disk_failures:xml=True || true
        fab alba.smoke_test
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
