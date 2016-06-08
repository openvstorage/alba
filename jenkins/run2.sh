#!/bin/bash -xue

env | sort

export WORKSPACE=$PWD
echo ${WORKSPACE}
export DRIVER=./setup/setup.native

eval `${opam_env}`

make clean
make

ldd ./ocaml/alba.native

if (${ALBA_USE_ETCD:-false} -eq true)
then
    export ALBA_ETCD=127.0.0.1:5000/albas/xxxx/
fi

if (${ALBA_USE_GIOEXECFILE:-false} -eq true)
then
    export ALBA_BLOB_IO_ENGINE=GioExecFile,${WORKSPACE}/cfg/gioexecfile.conf

fi

id
ulimit -n 1024
ulimit -c unlimited
cat /proc/sys/kernel/core_pattern

case "$1" in
    asd_start)
        ${DRIVER} asd_start || true
        ;;
    cpp)
        ./jenkins/cpp/010-build_client.sh
        ${DRIVER} cpp  || true
        ;;
    ocaml)
        ${DRIVER} ocaml || true
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
        ${DRIVER} compat || true
        ;;
    everything_else)
        ${DRIVER} everything_else  || true
        ;;
    *)
        echo "invalid test suite specified..."
        exit1
esac
