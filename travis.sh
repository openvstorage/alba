#!/bin/bash -xue

APT_DEPENDS="libssl-dev libsnappy-dev \
             libgmp3-dev help2man g++-4.8 \
             libgcrypt11-dev \
             protobuf-compiler libjerasure-dev \
             build-essential automake autoconf yasm \
             procps python-pip \
             aspcud redis-server \
             libev-dev"
APT_OCAML_DEPENDS="ocaml ocaml-native-compilers camlp4-extra opam"
OPAM_DEPENDS="ocamlfind \
         ssl.0.5.2 \
         camlbz2.0.6.0 \
         snappy.0.1.0 \
         lwt.2.5.1 \
         camltc.0.9.2 \
         cstruct.1.7.0 \
         ctypes.0.4.1 \
         ctypes-foreign.0.4.0 \
         bisect.1.3 \
         ocplib-endian.0.8 \
         quickcheck.1.0.2 \
         uuidm.0.9.5 \
         zarith.1.3 \
         kinetic-client \
         tiny_json \
         cmdliner \
         ppx_deriving.3.3 ppx_deriving_yojson.2.4 \
         sexplib.113.00.00 \
         core.113.00.00 \
         conf-libev \
         uri \
         result \
         redis.0.3.1
"

export OPAMYES=1
export OPAMVERBOSE=1
export OPAMCOLOR=never

before_install () {
    echo "Running 'before_install' phase"

    date

    env | sort
    sudo add-apt-repository "deb http://apt.openvstorage.org unstable main" | sudo tee -a /etc/apt/sources.list
    sudo add-apt-repository "deb http://ppa.launchpad.net/afrank/boost/ubuntu trusty main" | sudo tee -a /etc/apt/sources.list

    sudo add-apt-repository "deb http://us-central1.gce.archive.ubuntu.com/ubuntu trusty-backports main restricted universe multiverse"
    sudo add-apt-repository --yes ppa:avsm/ocaml42+opam12

    echo "Updating Apt cache"
    ATTEMPT=0
    until sudo timeout -s 9 30 apt-get update -q || [ ${ATTEMPT} -eq 3 ]; do
        echo "Attempt ${ATTEMPT} to update apt failed"
        sleep $(( ATTEMPT++ ))
    done

    echo "updating keys"
    sudo apt-key update

    echo "Installing general dependencies"
    sudo apt-get install -q --force-yes ${APT_DEPENDS} \
         ocaml ocaml-native-compilers camlp4-extra opam

    date

    opam init --auto-setup
    eval `opam config env`
    opam update

    echo "OCaml versions:"
    ocaml -version
    ocamlopt -version

    echo "Opam versions:"
    opam --version
    opam --git-version

    date

    echo "Installing ISA-L:"
    wget --no-check-certificate https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz
    tar xfzv isa-l-2.14.0.tar.gz
    cd isa-l-2.14.0
    ./configure
    make
    sudo make install

    date

    sudo pip install fabric junit-xml

    date

    sudo apt-get --force-yes -y install \
         libxio0 libxio-dev \
         libunwind8-dev libaio-dev libaio1 libaio1-dbg libaio-dev \
         libz-dev libbz2-dev \
         libgoogle-glog-dev libibverbs-dev \
         libboost1.57-all-dev libboost1.57-all

    date

    pushd .
    git clone  https://github.com/google/googletest
    cd googletest; mkdir build; cd build; cmake ..; sudo make install
    popd

    date

}

install () {
    echo "Running 'install' phase"

    date

    eval `opam config env`

    date

    opam install ${OPAM_DEPENDS} | tail
    opam depext arakoon.1.9.0
    opam install ${OPAM_DEPENDS} | tail

    date

    echo "Installing some specific arakoon"
    git clone https://github.com/openvstorage/arakoon.git
    cd arakoon
    git checkout tags/1.9.7
    make
    export PREFIX=/home/travis/.opam/system
    export OCAML_LIBDIR=`ocamlfind printconf destdir`
    make
    make uninstall_client install
    cd ..

    date



    echo "Installing ordma"
    sudo apt-get -y install librdmacm-dev
    apt-cache depends librdmacm-dev

    git clone https://github.com/toolslive/ordma.git
    cd ordma
    git checkout 263106bbcf7f8a9b1421da53a7e2a22db953bce9
    make install
    cd ..
    date

    echo "Installing specific orocksdb"
    git clone https://github.com/domsj/orocksdb.git
    cd orocksdb
    git checkout cb393e5c86d2f54ca1787b310764063f68f564e4
    ./install_rocksdb.sh

    pushd .
    cd rocksdb
    sudo make install
    popd

    make build install
    cd ..

    date

    echo "Installing gobjfs"
    pushd .
    git clone https://github.com/openvstorage/gobjfs.git
    cd gobjfs
    git pull
    git checkout c7d90c46939d86f4d47e1746b69e9d17d8654d98
    mkdir build
    cd build
    cmake -DCMAKE_BUILD_TYPE=RELWITHDEBINFO ..
    make | tail
    sudo make install
    popd

    echo "Installing etcd"
    curl -L  https://github.com/coreos/etcd/releases/download/v2.2.4/etcd-v2.2.4-linux-amd64.tar.gz -o etcd-v2.2.4-linux-amd64.tar.gz
    tar xzvf etcd-v2.2.4-linux-amd64.tar.gz
    sudo cp ./etcd-v2.2.4-linux-amd64/etcd    /usr/bin
    sudo cp ./etcd-v2.2.4-linux-amd64/etcdctl /usr/bin

    date
    pwd
    eval `opam config env`
    export ALBA_HOME=$(pwd)
    ./jenkins/system2/020-build_ocaml.sh

    date
}

script () {
    echo "Running 'script' phase"

    date

    eval `opam config env`
    export ARAKOON_BIN=arakoon
    export WORKSPACE=$(pwd)
    export TEST_DRIVER=./setup/setup.native
    export LD_LIBRARY_PATH=/usr/local/lib
    env | sort

    ./ocaml/alba.native version

    case "$SUITE" in
        build)
            true
            ;;
        system2)
            ${TEST_DRIVER} ocaml | tail -n1000
            X=$PIPESTATUS
            cat testresults.xml
            expr $X && false
            ;;
        disk_failures)
            ${TEST_DRIVER} disk_failures
            ;;
        recovery)
            find cfg/*.ini -exec sed -i "s,/tmp,${WORKSPACE}/tmp,g" {} \;
            fab dev.run_tests_recovery
            ;;
        cpp)
            g++ --version
            uname -a
            export CXX=g++-4.8
            ./jenkins/cpp/010-build_client.sh
            ${TEST_DRIVER} cpp
            fab alba.smoke_test
            ;;
        *)
            echo "invalid test suite specified..."
            exit1
    esac

    date
}

case "$1" in
    before_install)
        before_install
        ;;
    install)
        install
        ;;
    script)
        script
        ;;
    *)
        echo "Usage: $0 {before_install|install|script}"
        exit 1
esac
