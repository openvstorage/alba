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

RUN useradd jenkins -u 1001 -g root

RUN mkdir /home/alba
RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh
RUN sh ./opam_installer.sh /usr/local/bin 4.02.3

ENV opam_env='opam config env --root=/home/alba/OPAM'
RUN opam init --root=/home/alba/OPAM --comp 4.02.3



RUN rpm -iUvh http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm
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
        ppx_deriving.3.1 \
        ppx_deriving_yojson \
        core.113.00.00 \
        redis \
        uri.1.9.1 \
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

# install specific arakoon.
RUN git clone https://github.com/openvstorage/arakoon.git
RUN cd arakoon && git pull && git checkout tags/1.9.5
RUN cd arakoon && eval `${opam_env}` && make
RUN cd arakoon && eval `${opam_env}` \
    && export PREFIX=${opam_root}/4.02.3 \
    && export OCAML_LIBDIR=`ocamlfind printconf destdir` \
    && make install

# workarounds for limited rocksdb builder in orocksdb
RUN ln -s /usr/bin/g++ /usr/bin/g++-4.8
RUN yum -y install sudo
#
# Disable "ssh hostname sudo <cmd>", because it will show the password in clear.
#         You have to run "ssh -t hostname sudo <cmd>".
#
# Defaults    requiretty          # is line 56
RUN awk 'NR == 56 {next} {print}' /etc/sudoers >/tmp/__sudoers && mv /tmp/__sudoers /etc/sudoers

# install specific orocksdb
RUN git clone https://github.com/domsj/orocksdb.git \
    && eval `${opam_env}` \
    && cd orocksdb \
    && git checkout 8bc61d8a451a2724399247abf76643aa7b2a07e9 \
    && ./install_rocksdb.sh \
    && make build install

# be uniform on shared lib locations
RUN cp /usr/local/lib/librocksdb.so* /lib64/

#for now, instll ordma manually
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

RUN yum -y install iproute

ENV opam_env='opam config env --root=/home/alba/OPAM'
ENV ARAKOON_BIN=arakoon
CMD eval `${opam_env}` \
    && echo ${SUITE} \
    && ./jenkins/run.sh ${SUITE}
