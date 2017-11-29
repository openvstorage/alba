FROM ubuntu:14.04.3

RUN useradd jenkins -u 1500 -g root

RUN echo "deb http://archive.ubuntu.com/ubuntu/ trusty-backports main restricted universe multiverse" > /etc/apt/sources.list.d/trusty-backports-universe.list

RUN echo "deb http://ppa.launchpad.net/afrank/boost/ubuntu trusty main" \
    > /etc/apt/sources.list.d/boost_repo.list
RUN echo "deb-src http://ppa.launchpad.net/afrank/boost/ubuntu trusty main" \
    > /etc/apt/sources.list.d/boost_src_repo.list
RUN echo "deb http://apt.openvstorage.org unstable main" > /etc/apt/sources.list.d/ovsaptrepo.list

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    --allow-unauthenticated --force-yes \
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
        ncurses-dev \
        cmake \
        libgtest-dev \
        clang-3.5 \
        liblttng-ust0 librdmacm1 libtokyocabinet9 \
        libstdc++6:amd64 libzmq3 librabbitmq1 libomnithread3c2 libomniorb4-1 \
        libhiredis0.10 liblz4-1 libxio-dev libxio0 \
        omniorb-nameserver \
        libunwind8-dev libaio1 libaio1-dbg libaio-dev \
        libz-dev libbz2-dev \
        libgoogle-glog-dev libibverbs-dev \
        librdmacm-dev \
        software-properties-common

RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get update && apt-get install -y g++-5
RUN ln -sf /usr/bin/g++-5 /usr/bin/g++

# Install etcd:
RUN curl -L  https://github.com/coreos/etcd/releases/download/v2.2.4/etcd-v2.2.4-linux-amd64.tar.gz -o etcd-v2.2.4-linux-amd64.tar.gz
RUN tar xzvf etcd-v2.2.4-linux-amd64.tar.gz
RUN cp ./etcd-v2.2.4-linux-amd64/etcd /usr/bin \
    && cp ./etcd-v2.2.4-linux-amd64/etcdctl /usr/bin


# clone orocksdb & install rocksdb shared lib
RUN git clone https://github.com/domsj/orocksdb.git \
    && cd orocksdb \
    && git checkout tags/0.3.0 \
    && ./install_rocksdb.sh


RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh

env ocaml_version=4.04.2
RUN sh ./opam_installer.sh /usr/local/bin ${ocaml_version}

ENV opam_root=/home/jenkins/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root=${opam_root} --comp ${ocaml_version}

RUN eval `${opam_env}` && \
    opam repo add compat -k git https://github.com/toolslive/opam_anti_revisionism.git && \
    opam update -v && \
    opam install -y \
        oasis.0.4.10 \
        ocamlfind \
        omake.0.9.8.7 \
        ssl.0.5.3 \
        camlbz2 \
        snappy \
        sexplib \
        bisect \
        lwt_ssl.1.1.0 \
        lwt.3.0.0 \
        camltc.0.9.4 \
        ocplib-endian.1.0 \
        ctypes \
        ctypes-foreign \
        uuidm \
        zarith \
        mirage-no-xen.1 \
        quickcheck.1.0.2 \
        ounit.2.0.0 \
        cmdliner \
        conf-libev \
        depext \
        kinetic-client \
        cryptokit \
        tiny_json.1.1.4 \
        ppx_deriving.4.1 \
        ppx_deriving_yojson \
        base.v0.9.3 \
        core.v0.9.1 \
        redis.0.3.3 \
        uri.1.9.4 \
        piqi \
        result \
        ezxmlm

RUN wget https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz && \
    tar xfzv isa-l-2.14.0.tar.gz && \
    cd isa-l-2.14.0 && ./autogen.sh && ./configure && \
    make && make install && \
    cd .. && \
    rm -rf isa-l-2.14.0

# c++
RUN cd /usr/src/gtest \
        && cmake . \
        && make \
        && mv libg* /usr/lib/

# install specific arakoon.
RUN git clone https://github.com/openvstorage/arakoon.git
RUN cd arakoon && git pull && git checkout tags/1.9.22
RUN cd arakoon && eval `${opam_env}` && make
RUN cd arakoon && eval `${opam_env}` \
    && export PREFIX=${opam_root}/${ocaml_version} \
    && export OCAML_LIBDIR=`ocamlfind printconf destdir` \
    && make install

# install orocksdb
RUN eval `${opam_env}` \
    && cd orocksdb \
    && make build install


#for now, install ordma manually
RUN git clone https://github.com/toolslive/ordma.git \
    && cd ordma \
    && git checkout tags/0.0.2 \
    && eval `${opam_env}` \
    && make install


# Install alba 0.6, and arakoon.1.8.9 which we might need for compatibility tests
RUN echo "deb http://apt.openvstorage.org chicago-community main" > /etc/apt/sources.list.d/ovsaptrepo.list
RUN apt-get update && apt-get install -y --force-yes alba arakoon liburiparser1 gdb
RUN ln -s /usr/bin/alba /usr/bin/alba.0.6


## installing voldrv packages only works from within the OVS LAN

ARG INSTALL_VOLDRV_PACKAGES=false

RUN test ${INSTALL_VOLDRV_PACKAGES} = 'true' || echo 'we are NOT going to install the voldrv packages/testers'

# http://10.100.129.100:8080/view/volumedriver/view/ubuntu/job/volumedriver-no-dedup-release-ubuntu-14.04/37/artifact/volumedriver-core/build/debian/volumedriver-no-dedup-base_6.10.0-0_amd64.deb

ENV voldrv_jenkins=http://10.100.129.100:8080/view/volumedriver/view/ubuntu/job/volumedriver-no-dedup-release-ubuntu-14.04/37/artifact/volumedriver-core/build/debian

ENV voldrv_base_pkg_name=volumedriver-no-dedup
ENV voldrv_version=6.10.0-0_amd64

RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-base_${voldrv_version}.deb \
        && dpkg -i ${voldrv_base_pkg_name}-base_${voldrv_version}.deb )
RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-pitreplication_${voldrv_version}.deb \
        && dpkg -i ${voldrv_base_pkg_name}-pitreplication_${voldrv_version}.deb )
RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-server_${voldrv_version}.deb \
        && dpkg -i ${voldrv_base_pkg_name}-server_${voldrv_version}.deb )
RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-test_${voldrv_version}.deb \
        && dpkg -i ${voldrv_base_pkg_name}-test_${voldrv_version}.deb )

# packages with debug symbols
# RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
#     || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-base-dbgsym_${voldrv_version}.ddeb \
#         && dpkg -i ${voldrv_base_pkg_name}-base-dbgsym_${voldrv_version}.ddeb )
# RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
#     || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-pitreplication-dbgsym_${voldrv_version}.ddeb \
#         && dpkg -i ${voldrv_base_pkg_name}-pitreplication-dbgsym_${voldrv_version}.ddeb )
# RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
#     || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-server-dbgsym_${voldrv_version}.ddeb \
#         && dpkg -i ${voldrv_base_pkg_name}-server-dbgsym_${voldrv_version}.ddeb )
# RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
#     || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-test-dbgsym_${voldrv_version}.ddeb \
#         && dpkg -i ${voldrv_base_pkg_name}-test-dbgsym_${voldrv_version}.ddeb )


RUN pip install setuptools==34.0.1
RUN pip install fabric junit-xml

RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.profile"

RUN su - -c "echo 'LD_LIBRARY_PATH=/usr/local/lib; export LD_LIBRARY_PATH;' >> /home/jenkins/.profile"
RUN su - -c "echo 'VOLDRV_TEST=volumedriver_test; export VOLDRV_TEST;' >> /home/jenkins/.profile"
RUN su - -c "echo 'VOLDRV_BACKEND_TEST=backend_test; export VOLDRV_BACKEND_TEST;' >> /home/jenkins/.profile"

RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins

ENTRYPOINT ["/bin/bash", "-c", "set -e && /home/jenkins/alba/docker/docker-entrypoint.sh $@"]
