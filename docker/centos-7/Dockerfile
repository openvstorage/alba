FROM centos:7

RUN rpm --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
RUN yum -y install wget
RUN yum -y install curl
RUN yum -y install make m4 gcc patch unzip
RUN yum -y install git rsync mercurial
RUN yum -y install gcc-c++
RUN yum -y install bzip2-devel libffi-devel snappy-devel libev-devel
RUN yum -y install python-devel

# TODO: What about darcs ?

RUN useradd jenkins -u 1500 -g root

RUN rpm -iUvh http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-8.noarch.rpm
RUN yum -y update
RUN yum -y install python-pip openssl-devel gmp-devel which zlib-devel ncurses-devel bzip2


# protobuf
RUN wget https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.bz2 -O protobuf-2.6.1.tar.bz2 \
    && tar -jxvf protobuf-2.6.1.tar.bz2 \
    && cd protobuf-2.6.1 \
    && ./configure && make && make install

#RUN rpm -ivh http://cbs.centos.org/kojifiles/packages/protobuf/2.5.0/10.el7.centos/src/protobuf-2.5.0-10.el7.centos.src.rpm

RUN pip install fabric junit-xml

RUN wget http://cudf-solvers.irill.org/cudf_remote_proxy
RUN chmod u+x cudf_remote_proxy
RUN mv cudf_remote_proxy /usr/local/bin/

ENV OPAMEXTERNALSOLVER="cudf_remote_proxy %{input}% %{output}% %{criteria}%"

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
        redis.0.3.1 \
        uri.1.9.2 \
        result


RUN yum -y install autoconf help2man perl-Thread-Queue

# AUTOMAKE-1.14.1 (for YASM)
RUN wget http://ftp.gnu.org/gnu/automake/automake-1.14.1.tar.xz \
        && tar -xvf automake-1.14.1.tar.xz \
        && cd automake-1.14.1 \
        && ./configure \
        && make \
        && make install

#YASM
RUN yum -y install libtool
RUN git clone --depth 1 git://github.com/yasm/yasm.git
RUN cd yasm && autoreconf -fiv && ./configure && make && make install && make distclean


RUN wget https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz
RUN tar xfzv isa-l-2.14.0.tar.gz
RUN cd isa-l-2.14.0 && ./configure
RUN cd isa-l-2.14.0 && make
RUN cd isa-l-2.14.0 && make install


RUN yum -y install cmake3 sudo

# make sudo work:
#
# Disable "ssh hostname sudo <cmd>", because it will show the password in clear.
#         You have to run "ssh -t hostname sudo <cmd>".
#
# Defaults    requiretty          # is line 56
RUN awk 'NR == 56 {next} {print}' /etc/sudoers >/tmp/__sudoers && mv /tmp/__sudoers /etc/sudoers

RUN echo '%_install_langs C' >> /etc/rpm/macros && \
    yum -y install epel-release && \
    echo -e '[ovs]\nname=ovs\nbaseurl=http://yum.openvstorage.org/CentOS/7/x86_64/dists/unstable\nenabled=1\ngpgcheck=0' > /etc/yum.repos.d/ovs.repo && \
    yum -y update && \
    yum -y install  \
                   boost-devel \
                   boost-static \
                   boost-log \
                   libaio-devel \
                   librdmacm-devel \
                   gflags-devel \
                   glog-devel \
                   libunwind \
                   libxio \
                   libxio-devel\
    && \
    yum clean all && \
    rm -rf /usr/share/doc/*
RUN ln -s /usr/lib64/libunwind.so.8 /usr/lib64/libunwind.so

# install gtest TODO:specific version ?
RUN git clone  https://github.com/google/googletest \
    && cd googletest &&  mkdir build && cd build && cmake3 .. \
    && sudo make install
RUN cd googletest && git log --oneline | head -n 5

# libgtest.a ends up in /usr/local/lib/
RUN cp /usr/local/lib/libgtest.a /lib64/

# install specific arakoon.
RUN git clone https://github.com/openvstorage/arakoon.git
RUN cd arakoon && git pull && git checkout tags/1.9.10
RUN cd arakoon && eval `${opam_env}` && make
RUN cd arakoon && eval `${opam_env}` \
    && export PREFIX=${opam_root}/${ocaml_version} \
    && export OCAML_LIBDIR=`ocamlfind printconf destdir` \
    && make install

# workarounds for limited rocksdb builder in orocksdb
RUN ln -s /usr/bin/g++ /usr/bin/g++-4.8



# install specific orocksdb
RUN git clone https://github.com/domsj/orocksdb.git \
    && eval `${opam_env}` \
    && cd orocksdb \
    && git checkout 26c45963f1f305825785592efb41b50192a07491 \
    && ./install_rocksdb.sh \
    && cp /usr/local/lib/librocksdb.so* /lib64/ \
    && sudo ldconfig -v \
    && make build install

#for now, install ordma manually
RUN yum -y install librdmacm-devel
RUN git clone https://github.com/toolslive/ordma.git \
    && cd ordma \
    && git checkout tags/0.0.2 \
    && eval `${opam_env}` \
    && make install

RUN yum -y install rpm-build
RUN yum -y install libgcrypt-devel

#gf-complete
RUN rpm -ivv http://people.redhat.com/zaitcev/tmp/gf-complete-1.02-1.fc20.src.rpm
RUN rpmbuild -ba /root/rpmbuild/SPECS/gf-complete.spec
RUN rpm -i /root/rpmbuild/RPMS/x86_64/gf-complete-1.02-1.el7.centos.x86_64.rpm
RUN rpm -i /root/rpmbuild/RPMS/x86_64/gf-complete-devel-1.02-1.el7.centos.x86_64.rpm

#jerasure
RUN rpm -ivv http://people.redhat.com/zaitcev/tmp/jerasure-2.0-1.fc20.src.rpm
RUN rpmbuild -ba /root/rpmbuild/SPECS/jerasure.spec
RUN rpm -i /root/rpmbuild/RPMS/x86_64/jerasure-2.0-1.el7.centos.x86_64.rpm
RUN rpm -i /root/rpmbuild/RPMS/x86_64/jerasure-devel-2.0-1.el7.centos.x86_64.rpm
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins


# gobjfs shared lib:

RUN git clone https://github.com/openvstorage/gobjfs.git
RUN cd gobjfs  && git pull && git checkout 3b591baf7518987ce1b6c828865f0089007281e4
RUN cd gobjfs \
       && mkdir build \
       && cd build \
       && cmake3 -DCMAKE_BUILD_TYPE=RELWITHDEBINFO .. \
       && make \
       && make install

ENV LD_LIBRARY_PATH=/usr/lib64:/usr/local/lib
ENV GOBJFS_HOME=/gobjfs/

RUN yum -y install iproute

RUN pip install fabric junit-xml

RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.bash_profile"
RUN su - -c "echo 'LD_LIBRARY_PATH=/usr/local/lib; export LD_LIBRARY_PATH;' >> /home/jenkins/.bash_profile"

ENTRYPOINT ["/home/jenkins/alba/docker/docker-entrypoint.sh"]
