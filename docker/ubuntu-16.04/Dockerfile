FROM ubuntu:16.04

RUN useradd jenkins -u 1500 -g root

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
        sysstat \
        ncurses-dev

RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh

env ocaml_version=4.03.0
RUN sh ./opam_installer.sh /usr/local/bin ${ocaml_version}

ENV opam_root=/home/jenkins/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root=${opam_root} --comp ${ocaml_version}

RUN git clone git://github.com/janestreet/opam-repository.git /janestreet-opam-repo && \
    cd /janestreet-opam-repo && \
    git checkout b98fd1964856f0c0b022a42ec4e6fc6c7bad2e81
RUN eval `${opam_env}` && \
    opam repo --root=${opam_root} -k local add janestreet /janestreet-opam-repo

RUN eval `${opam_env}` && \
    opam update && \
    opam install -y \
        ocamlfind \
        ssl.0.5.2 \
        camlbz2 \
        snappy \
        sexplib \
        bisect \
        lwt.2.5.2 \
        camltc.0.9.3 \
        ocplib-endian.1.0 \
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
        ppx_deriving \
        ppx_deriving_yojson \
        core.114.05+21 \
        redis.0.3.1 \
        uri.1.9.2 \
        result

RUN wget https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz && \
    tar xfzv isa-l-2.14.0.tar.gz && \
    cd isa-l-2.14.0 && ./autogen.sh && ./configure && \
    make && make install && \
    cd .. && \
    rm -rf isa-l-2.14.0

# c++
RUN apt-get update && apt-get -y install libgtest-dev cmake
RUN cd /usr/src/gtest \
        && cmake . \
        && make \
        && mv libg* /usr/lib/


# install specific arakoon.
RUN git clone https://github.com/openvstorage/arakoon.git
RUN cd arakoon && git pull && git checkout tags/1.9.9
RUN cd arakoon && eval `${opam_env}` && make
RUN cd arakoon && eval `${opam_env}` \
    && export PREFIX=${opam_root}/${ocaml_version} \
    && export OCAML_LIBDIR=`ocamlfind printconf destdir` \
    && make install

# install specific orocksdb
RUN git clone https://github.com/domsj/orocksdb.git \
    && eval `${opam_env}` \
    && cd orocksdb \
    && git checkout 26c45963f1f305825785592efb41b50192a07491 \
    && ./install_rocksdb.sh \
    && make build install

#for now, install ordma manually
RUN apt-get update && apt-get -y install librdmacm-dev
RUN git clone https://github.com/toolslive/ordma.git \
    && cd ordma \
    && git checkout tags/0.0.2 \
    && eval `${opam_env}` \
    && make install

run apt-get update && apt-get -y --allow-unauthenticated install \
    libboost1.58-all-dev libboost1.58-all

RUN echo "deb http://archive.ubuntu.com/ubuntu/ wily universe" > /etc/apt/sources.list.d/wily-universe.list

RUN echo "deb http://apt.openvstorage.org unstable main" > /etc/apt/sources.list.d/ovsaptrepo.list
RUN apt-get update && apt-get -y --allow-unauthenticated install \
      clang-3.5 \
      liblttng-ust0 librdmacm1 libtokyocabinet9 \
      libstdc++6:amd64 libzmq3 librabbitmq1 libomnithread3c2 libomniorb4-1 \
      libhiredis0.13 liblz4-1 libxio-dev libxio0 \
      omniorb-nameserver \
      libunwind8-dev libaio1 libaio1-dbg libaio-dev \
      libz-dev libbz2-dev \
      libgoogle-glog-dev libibverbs-dev

RUN git clone https://github.com/openvstorage/gobjfs.git
RUN cd gobjfs  && git pull && git checkout 3b591baf7518987ce1b6c828865f0089007281e4
RUN cd gobjfs \
       && mkdir build \
       && cd build \
       && cmake -DCMAKE_BUILD_TYPE=RELWITHDEBINFO .. \
       && make \
       && make install

# Install etcd:
RUN curl -L  https://github.com/coreos/etcd/releases/download/v2.2.4/etcd-v2.2.4-linux-amd64.tar.gz -o etcd-v2.2.4-linux-amd64.tar.gz
RUN tar xzvf etcd-v2.2.4-linux-amd64.tar.gz
RUN cp ./etcd-v2.2.4-linux-amd64/etcd /usr/bin \
    && cp ./etcd-v2.2.4-linux-amd64/etcdctl /usr/bin

RUN pip install fabric junit-xml

RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.profile"
RUN su - -c "echo 'LD_LIBRARY_PATH=/usr/local/lib; export LD_LIBRARY_PATH;' >> /home/jenkins/.profile"
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins

ENTRYPOINT ["/home/jenkins/alba/docker/docker-entrypoint.sh"]
