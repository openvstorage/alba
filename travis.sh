#!/bin/bash -xue

APT_DEPENDS="libev-dev libssl-dev libsnappy-dev \
             libgmp3-dev help2man g++-4.8 \
             libgcrypt11-dev \
             protobuf-compiler libjerasure-dev \
             build-essential automake autoconf yasm \
             python-pip"
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
         sexplib.112.35.00 \
         uuidm.0.9.5 \
         zarith.1.3 \
         arakoon.1.8.8 \
         orocksdb.0.2.1 \
         kinetic-client \
         tiny_json \
         cmdliner \
         ppx_deriving ppx_deriving_yojson         
"

export OPAMYES=1
export OPAMVERBOSE=1
export OPAMCOLOR=never

before_install () {
    echo "Running 'before_install' phase"

    echo "Adding PPA"
    sudo add-apt-repository "deb mirror://mirrors.ubuntu.com/mirrors.txt trusty main restricted universe multiverse"
    sudo add-apt-repository "deb mirror://mirrors.ubuntu.com/mirrors.txt trusty-updates main restricted universe multiverse"
    sudo add-apt-repository "deb mirror://mirrors.ubuntu.com/mirrors.txt trusty-backports main restricted universe multiverse"
    sudo add-apt-repository --yes ppa:avsm/ocaml42+opam12

    echo "Updating Apt cache"
    sudo apt-get update -qq

    echo "Installing general dependencies"
    sudo apt-get install -qq ${APT_DEPENDS}
    echo "Installing dependencies"
    sudo apt-get install -qq ${APT_OCAML_DEPENDS}

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

    opam install ${OPAM_DEPENDS} || true
    opam depext arakoon.1.8.6 orocksdb.0.2.0
    opam install ${OPAM_DEPENDS}

    ./jenkins/system2/020-build_ocaml.sh
}

script () {
    echo "Running 'script' phase"

    case "$SUITE" in
        build)
            ./ocaml/alba.native version
            ;;
        system2)
            fab dev.run_tests_ocaml:xml=True || true
            ./jenkins/system2/050-smoke_test.sh
            ;;
        *)
            echo "invalid suite specified.."
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
