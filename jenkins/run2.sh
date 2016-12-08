#!/bin/bash -xue

env | sort

export WORKSPACE=$PWD
export ALBA_HOME=$PWD
echo ${WORKSPACE}
export DRIVER=./setup/setup.native

export ARAKOON_BIN=$(which arakoon)


if [ -t 1 ] || [[ ${1-bash} == "test_integrate_"* ]]
then true;
else
    # this path is taken on jenkins, clean previous builds first
    make clean
    if [[ ${1-bash} != "package_"* ]]
    then make
    fi
fi

if (${ALBA_USE_ETCD:-false} -eq true)
then
    export ALBA_ETCD=127.0.0.1:5000/albas/xxxx/
fi

id
ulimit -n 1024
ulimit -c unlimited
cat /proc/sys/kernel/core_pattern

function package_debian {
    DEB_BUILD_OPTIONS=nostrip fakeroot debian/rules clean build binary
    created_package=`ls -t alba_*_amd64.deb | head -n1`
    new_package=alba_`git describe --tags --dirty | xargs`_amd64.deb
    if [ $created_package == $new_package ]
    then echo "point release"
    else mv ${created_package} ${new_package}
    fi
}

function check_results {
    if [[ "${TRAVIS-false}" == "true" ]] && ! grep "errors=\"0\" failures=\"0\"" testresults.xml
    then
        cat testresults.xml
        exit 1
    fi
}

case "${1-bash}" in
    asd_start)
        ${DRIVER} asd_start || true
        check_results
        ;;
    cpp)
        ./jenkins/cpp/010-build_client.sh
        ${DRIVER} cpp  || true
        check_results
        ;;
    ocaml)
        ${DRIVER} ocaml || true
        check_results
        ;;
    stress)
        ${DRIVER} stress || true
        check_results
        ;;
    voldrv_backend)
        ${DRIVER} voldrv_backend || true
        check_results
        ;;
    voldrv_tests)
        ${DRIVER} voldrv_tests || true
        check_results
        ;;
    disk_failures)
        ${DRIVER} disk_failures || true
        check_results
        ;;
    compat)
        ${DRIVER} compat || true
        check_results
        ;;
    recovery)
        ${DRIVER} recovery || true
        ;;
    everything_else)
        ${DRIVER} everything_else  || true
        check_results
        ;;
    test_integrate_deb)
        ./jenkins/run.sh test_integrate_deb
        ;;
    test_integrate_rpm)
        ./jenkins/run.sh test_integrate_rpm
        ;;
    bash)
        bash
        ;;
    clean)
        make clean
        ;;
    build)
        make
        ;;
    package_deb)
        package_debian
        ;;
    package_rpm)
        ./jenkins/package_rpm/030-package.sh
        ;;
    *)
        echo "invalid test suite specified..."
        exit1
esac
