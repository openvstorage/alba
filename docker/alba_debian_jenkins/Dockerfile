FROM ubuntu:14.04.3
RUN echo "deb http://archive.ubuntu.com/ubuntu/ trusty-backports main restricted universe multiverse" > /etc/apt/sources.list.d/trusty-backports-universe.list
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
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
        fuse \
        sysstat

RUN useradd jenkins -u 1001 -g root

RUN wget http://ppa.launchpad.net/anatol/tup/ubuntu/pool/main/t/tup/tup_0.7.2.12+ga582fee_amd64.deb
RUN sudo dpkg -i tup_0.7.2.12+ga582fee_amd64.deb

RUN pip install fabric junit-xml

RUN mkdir /home/alba
RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh
RUN sh ./opam_installer.sh /usr/local/bin 4.02.3
env opam_root=/home/alba/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root=/home/alba/OPAM --comp 4.02.3
RUN eval `${opam_env}` && \
    opam update && \
    opam install -y \
        ocamlfind \
        ssl.0.5.2 \
        camlbz2 \
        snappy \
        sexplib \
        bisect \
        lwt.2.5.1 \
        camltc \
        cstruct \
        ctypes \
        ctypes-foreign \
        uuidm \
        zarith \
        mirage-no-xen.1 \
        quickcheck.1.0.2 \
        cmdliner \
        conf-libev \
        depext \
        kinetic-client \
        tiny_json \
        ppx_deriving_yojson \
        core.113.00.00 \
        redis \
        uri.1.9.1

RUN wget https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz
RUN tar xfzv isa-l-2.14.0.tar.gz
RUN cd isa-l-2.14.0 && ./configure
RUN cd isa-l-2.14.0 && make
RUN cd isa-l-2.14.0 && make install

# c++
RUN apt-get update && apt-get -y install libgtest-dev cmake
RUN cd /usr/src/gtest \
        && cmake . \
        && make \
        && mv libg* /usr/lib/


RUN eval `${opam_env}` && opam update

# install specific arakoon.
RUN git clone https://github.com/openvstorage/arakoon.git
RUN cd arakoon && git pull && git checkout tags/1.9.1
RUN cd arakoon && eval `${opam_env}` && make
RUN cd arakoon && eval `${opam_env}` \
    && export PREFIX=${opam_root}/4.02.3 \
    && export OCAML_LIBDIR=`ocamlfind printconf destdir` \
    && make install

RUN eval `${opam_env}` && \
    opam update && \
    opam depext orocksdb.0.2.2 && \
    opam install -y orocksdb.0.2.2

RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins

RUN apt-get update && apt-get -y install clang-3.5

RUN echo "deb http://apt.openvstorage.org chicago-community main" > /etc/apt/sources.list.d/ovsaptrepo.list
RUN apt-get update && apt-get -y --force-yes install \
      libboost1.57-all liblttng-ust0 librdmacm1 libtokyocabinet9 \
      libstdc++6:amd64 libzmq3 librabbitmq1 libomnithread3c2 libomniorb4-1

## This only works from inside the OVS lan...
RUN wget http://10.100.129.100:8080/view/voldrv/job/volumedriver-5.0.x-release-ubuntu-14.04/5/artifact/volumedriver-core/build/debian/volumedriver-base_5.0.7-0_amd64.deb && dpkg -i volumedriver-base_5.0.7-0_amd64.deb
RUN wget http://10.100.129.100:8080/view/voldrv/job/volumedriver-5.0.x-release-ubuntu-14.04/5/artifact/volumedriver-core/build/debian/volumedriver-pitreplication_5.0.7-0_amd64.deb && dpkg -i volumedriver-pitreplication_5.0.7-0_amd64.deb
RUN wget http://10.100.129.100:8080/view/voldrv/job/volumedriver-5.0.x-release-ubuntu-14.04/5/artifact/volumedriver-core/build/debian/volumedriver-server_5.0.7-0_amd64.deb && dpkg -i volumedriver-server_5.0.7-0_amd64.deb
RUN wget http://10.100.129.100:8080/view/voldrv/job/volumedriver-5.0.x-release-ubuntu-14.04/5/artifact/volumedriver-core/build/debian/volumedriver-test_5.0.7-0_amd64.deb && dpkg -i volumedriver-test_5.0.7-0_amd64.deb

# Install alba 0.6, and arakoon.1.8.9 which we might need for compatibility tests
RUN apt-get update && apt-get install -y --force-yes alba arakoon
RUN ln -s /usr/bin/alba /usr/bin/alba.0.6
ENV UID 1001
ENV ARAKOON_BIN=arakoon
ENV VOLDRV_TEST=volumedriver_test
ENV VOLDRV_BACKEND_TEST=backend_test

# Install etcd:
RUN curl -L  https://github.com/coreos/etcd/releases/download/v2.2.4/etcd-v2.2.4-linux-amd64.tar.gz -o etcd-v2.2.4-linux-amd64.tar.gz
RUN tar xzvf etcd-v2.2.4-linux-amd64.tar.gz
RUN cp ./etcd-v2.2.4-linux-amd64/etcd /usr/bin \
    && cp ./etcd-v2.2.4-linux-amd64/etcdctl /usr/bin


CMD eval `${opam_env}` \
    && echo ${SUITE} \
    && ./jenkins/run.sh ${SUITE}
