FROM ubuntu:15.04
RUN apt-get -y update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential m4 apt-utils \
        libffi-dev libssl-dev \
        libbz2-dev \
        libgmp3-dev \
        libev-dev \
        libsnappy-dev \
        libxen-dev \
        help2man \
        pkg-config \
        time \
        aspcud \
        wget \
        rsync \
        darcs \
        git \
        unzip \
        protobuf-compiler \
        libgcrypt20-dev \
        libjerasure-dev \
        yasm \
        automake \
        python-dev \
        python-pip \
        debhelper \
        psmisc \
        strace \
        curl \
        g++ \
        libgflags-dev \
        sudo \
        libtool \
        libboost-all-dev \
        fuse

RUN useradd jenkins -u 1001 -g root

RUN wget http://ppa.launchpad.net/anatol/tup/ubuntu/pool/main/t/tup/tup_0.7.2.12+ga582fee_amd64.deb
RUN sudo dpkg -i tup_0.7.2.12+ga582fee_amd64.deb

RUN pip install fabric junit-xml

RUN mkdir /home/alba
RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh
RUN sh ./opam_installer.sh /usr/local/bin 4.02.1

ENV opam_env='opam config env --root=/home/alba/OPAM'
RUN opam init --root=/home/alba/OPAM --comp 4.02.1
RUN eval `${opam_env}` && opam install -y \
        ocamlfind \
        ssl \
        camlbz2 \
        snappy \
        sexplib \
        bisect \
        lwt.2.5.0 \
        camltc \
        cstruct \
        ctypes \
        ctypes-foreign \
        uuidm \
        zarith \
        mirage-no-xen.1 \
        quickcheck.1.0.2 \
        nocrypto.0.5.1 \
        cmdliner \
        conf-libev \
        depext \
        kinetic-client \
        tiny_json \
        ppx_deriving_yojson \
        core_kernel

RUN wget https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz
RUN tar xfzv isa-l-2.14.0.tar.gz
RUN cd isa-l-2.14.0 && ./configure
RUN cd isa-l-2.14.0 && make
RUN cd isa-l-2.14.0 && make install

RUN eval `${opam_env}` && opam install -y arakoon.1.8.8
RUN eval `${opam_env}` && opam depext orocksdb.0.2.1
RUN eval `${opam_env}` && opam install -y orocksdb.0.2.1
RUN cd /home/alba && git clone https://github.com/openvstorage/alba.git
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins
CMD eval `${opam_env}` && opam list
