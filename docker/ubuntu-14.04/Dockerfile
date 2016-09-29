FROM ubuntu:14.04.3

RUN useradd jenkins -u 1500 -g root

RUN echo "deb http://archive.ubuntu.com/ubuntu/ trusty-backports main restricted universe multiverse" > /etc/apt/sources.list.d/trusty-backports-universe.list

RUN echo "deb http://ppa.launchpad.net/afrank/boost/ubuntu trusty main" \
    > /etc/apt/sources.list.d/boost_repo.list
RUN echo "deb-src http://ppa.launchpad.net/afrank/boost/ubuntu trusty main" \
    > /etc/apt/sources.list.d/boost_src_repo.list

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --force-yes \
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
        libboost1.57-all-dev libboost1.57-all \
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
        oasis.0.4.6 \
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
        redis.0.3.3 \
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
RUN cd arakoon && git pull && git checkout 5f2d9f32c3d47e9fc1c02f82d9127c607dfe43b9
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


RUN echo "deb http://apt.openvstorage.org unstable main" > /etc/apt/sources.list.d/ovsaptrepo.list
RUN apt-get update && apt-get -y --allow-unauthenticated install \
      clang-3.5 \
      liblttng-ust0 librdmacm1 libtokyocabinet9 \
      libstdc++6:amd64 libzmq3 librabbitmq1 libomnithread3c2 libomniorb4-1 \
      libhiredis0.10 liblz4-1 libxio-dev libxio0 \
      omniorb-nameserver \
      libunwind8-dev libaio1 libaio1-dbg libaio-dev \
      libz-dev libbz2-dev \
      libgoogle-glog-dev libibverbs-dev

#for now, install ordma manually
RUN apt-get update && apt-get -y install librdmacm-dev
RUN git clone https://github.com/toolslive/ordma.git \
    && cd ordma \
    && git checkout tags/0.0.2 \
    && eval `${opam_env}` \
    && make install


RUN git clone https://github.com/openvstorage/gobjfs.git
RUN cd gobjfs  && git pull && git checkout f5d20ec3556892512cf316ef11818398898c7576
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


# Install alba 0.6, and arakoon.1.8.9 which we might need for compatibility tests
RUN echo "deb http://apt.openvstorage.org chicago-community main" > /etc/apt/sources.list.d/ovsaptrepo.list
RUN apt-get update && apt-get install -y --force-yes alba arakoon liburiparser1 gdb
RUN ln -s /usr/bin/alba /usr/bin/alba.0.6


## This only works from inside the OVS lan...
# http://10.100.129.100:8080/job/volumedriver-no-dedup-conditional-backend-write-release-ubuntu-14.04/25/artifact/volumedriver-core/build/debian/libovsvolumedriver-dbgsym_6.1.0-dev.201609282340.e8592c6_amd64.ddeb
ENV voldrv_jenkins=http://10.100.129.100:8080/job/volumedriver-no-dedup-conditional-backend-write-release-ubuntu-14.04/25/artifact/volumedriver-core/build/debian
ENV voldrv_base_pkg_name=volumedriver-no-dedup
ENV voldrv_version=6.1.0-dev.201609282340.e8592c6_amd64

RUN wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-base_${voldrv_version}.deb \
    && dpkg -i ${voldrv_base_pkg_name}-base_${voldrv_version}.deb
RUN wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-pitreplication_${voldrv_version}.deb \
    && dpkg -i ${voldrv_base_pkg_name}-pitreplication_${voldrv_version}.deb
RUN wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-server_${voldrv_version}.deb \
    && dpkg -i ${voldrv_base_pkg_name}-server_${voldrv_version}.deb
RUN wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-test_${voldrv_version}.deb \
    && dpkg -i ${voldrv_base_pkg_name}-test_${voldrv_version}.deb

# packages with debug symbols
# RUN wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-base-dbgsym_${voldrv_version}.ddeb \
#     && dpkg -i ${voldrv_base_pkg_name}-base-dbgsym_${voldrv_version}.ddeb
# RUN wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-pitreplication-dbgsym_${voldrv_version}.ddeb \
#     && dpkg -i ${voldrv_base_pkg_name}-pitreplication-dbgsym_${voldrv_version}.ddeb
# RUN wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-server-dbgsym_${voldrv_version}.ddeb \
#     && dpkg -i ${voldrv_base_pkg_name}-server-dbgsym_${voldrv_version}.ddeb
# RUN wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-test-dbgsym_${voldrv_version}.ddeb \
#     && dpkg -i ${voldrv_base_pkg_name}-test-dbgsym_${voldrv_version}.ddeb


RUN pip install fabric junit-xml

RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.profile"

RUN su - -c "echo 'LD_LIBRARY_PATH=/usr/local/lib; export LD_LIBRARY_PATH;' >> /home/jenkins/.profile"
RUN su - -c "echo 'VOLDRV_TEST=volumedriver_test; export VOLDRV_TEST;' >> /home/jenkins/.profile"
RUN su - -c "echo 'VOLDRV_BACKEND_TEST=backend_test; export VOLDRV_BACKEND_TEST;' >> /home/jenkins/.profile"

RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins

ENTRYPOINT ["/home/jenkins/alba/docker/docker-entrypoint.sh"]
