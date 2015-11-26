#!/bin/bash -xue

env

export WORKSPACE=$PWD
echo ${WORKSPACE}

eval `${opam_env}`

make

find cfg/*.ini -exec sed -i "s,/tmp,${WORKSPACE}/tmp,g" {} \;

id
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
    *)
        echo "invalid test suite specified..."
        exit1
esac
