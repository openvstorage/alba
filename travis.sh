#!/bin/bash -xue

APT_DEPENDS="libssl-dev libsnappy-dev \
             libgmp3-dev help2man g++-4.8 \
             libgcrypt11-dev \
             protobuf-compiler libjerasure-dev \
             build-essential automake autoconf yasm \
             procps python-pip \
             aspcud"
APT_OCAML_DEPENDS="ocaml ocaml-native-compilers camlp4-extra opam"
OPAM_DEPENDS="ocamlfind \
         ssl.0.5.0 \
         camlbz2.0.6.0 \
         snappy.0.1.0 \
         lwt.2.5.0 \
         camltc.0.9.2 \
         cstruct.1.7.0 \
         ctypes.0.4.1 \
         ctypes-foreign.0.4.0 \
         bisect.1.3 \
         ocplib-endian.0.8 \
         quickcheck.1.0.2 \
         nocrypto.0.5.1 \
         uuidm.0.9.5 \
         zarith.1.3 \
         arakoon.1.8.8 \
         orocksdb.0.2.1 \
         kinetic-client \
         tiny_json \
         cmdliner \
         ppx_deriving ppx_deriving_yojson \
         sexplib.112.35.00 \
         core_kernel.112.35.00 \
         cohttp
"

export OPAMYES=1
export OPAMVERBOSE=1
export OPAMCOLOR=never

before_install () {
    echo "Running 'before_install' phase"

    env

    echo "Adding PPA"
    sudo add-apt-repository "deb mirror://mirrors.ubuntu.com/mirrors.txt trusty main restricted universe multiverse"
    sudo add-apt-repository "deb mirror://mirrors.ubuntu.com/mirrors.txt trusty-updates main restricted universe multiverse"
    sudo add-apt-repository "deb mirror://mirrors.ubuntu.com/mirrors.txt trusty-backports main restricted universe multiverse"

    echo "Updating Apt cache"
    sudo apt-get update -q

    echo "Installing general dependencies"
    sudo apt-get install -q ${APT_DEPENDS}

    # using
    # sudo add-apt-repository --yes ppa:avsm/ocaml42+opam12
    # and then
    # sudo apt-get install ocaml ocaml-native-compilers camlp4-extra opam
    # no longer works because it results in 4.02.3...
    # so now using this alternative method:
    wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh
    sudo sh ./opam_installer.sh /usr/local/bin 4.02.1  # change to system once it works with 4.02.3
    /usr/local/bin/opam init --comp 4.02.1 |tail -n256 # change to system once it works with 4.02.3
    expr $PIPESTATUS && false
    eval `opam config env`
    opam update
    opam install camlp4

    echo "OCaml versions:"
    ocaml -version
    ocamlopt -version

    echo "Opam versions:"
    opam --version
    opam --git-version

    echo "Installing ISA-L:"
    wget https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz
    tar xfzv isa-l-2.14.0.tar.gz
    cd isa-l-2.14.0
    ./configure
    make
    sudo make install

    sudo pip install fabric junit-xml
}

install () {
    echo "Running 'install' phase"

    opam init
    eval `opam config env`
    opam update

    wget https://gist.github.com/domsj/f2d7726e5d9895d498fb/raw/1e6191c16cc45bbc493328188079cad40a7aa6c8/librocksdb.so.3.12.0
    sudo cp librocksdb.so.3.12.0 /usr/local/lib/librocksdb.so.3.12.0
    sudo ln -s /usr/local/lib/librocksdb.so.3.12.0 /usr/local/lib/librocksdb.so.3.12
    sudo ln -s /usr/local/lib/librocksdb.so.3.12.0 /usr/local/lib/librocksdb.so.3
    sudo ln -s /usr/local/lib/librocksdb.so.3.12.0 /usr/local/lib/librocksdb.so

    opam install ${OPAM_DEPENDS} || true
    opam depext arakoon.1.8.6 orocksdb.0.2.0
    opam install ${OPAM_DEPENDS}

    ./jenkins/system2/020-build_ocaml.sh
}

script () {
    echo "Running 'script' phase"

    eval `opam config env`
    export ARAKOON_BIN=arakoon

    ./ocaml/alba.native version

    case "$SUITE" in
        build)
            true
            ;;
        system2)
            fab dev.run_tests_ocaml | tail -n256
            expr $PIPESTATUS && false
            fab alba.smoke_test
            ;;
        disk_failures)
            fab dev.run_tests_disk_failures
            fab alba.smoke_test
            ;;
        recovery)
            fab dev.run_tests_recovery
            ;;
        cpp)
            g++ --version
            uname -a
            export CXX=g++-4.8
            sudo apt-get install -y libboost-all-dev # kitchen sink
            sudo apt-get install -y fuse
            sudo modprobe fuse
            wget http://ppa.launchpad.net/anatol/tup/ubuntu/pool/main/t/tup/tup_0.7.2.12+ga582fee_amd64.deb
            sudo dpkg -i tup_0.7.2.12+ga582fee_amd64.deb

            ./jenkins/cpp/010-build_client.sh
            fab dev.run_tests_cpp
            fab alba.smoke_test
            ;;
        *)
            echo "invalid test suite specified..."
            exit1
    esac
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
