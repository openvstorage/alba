FROM centos:7

RUN useradd jenkins -u 1500 -g root

RUN rpm --import file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
RUN rpm -iUvh http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-10.noarch.rpm

RUN yum -y update \
    && yum -y install wget curl \
                      make m4 gcc patch unzip \
                      git rsync mercurial \
                      gcc-c++ \
                      bzip2-devel libffi-devel snappy-devel libev-devel \
                      python-devel \
                      file \
                      python-pip openssl-devel gmp-devel which zlib-devel \
                      ncurses-devel bzip2 cmake3 sudo \
                      autoconf help2man perl-Thread-Queue \
                      libtool

# make sudo work:
#
# Disable "ssh hostname sudo <cmd>", because it will show the password in clear.
#         You have to run "ssh -t hostname sudo <cmd>".
#
# Defaults    requiretty          # is line 56
RUN awk 'NR == 56 {next} {print}' /etc/sudoers >/tmp/__sudoers && mv /tmp/__sudoers /etc/sudoers


# protobuf
RUN wget https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.bz2 -O protobuf-2.6.1.tar.bz2 \
    && tar -jxvf protobuf-2.6.1.tar.bz2 \
    && cd protobuf-2.6.1 \
    && ./configure && make && make install

#RUN rpm -ivh http://cbs.centos.org/kojifiles/packages/protobuf/2.5.0/10.el7.centos/src/protobuf-2.5.0-10.el7.centos.src.rpm

# workarounds for limited rocksdb builder in orocksdb
RUN ln -s /usr/bin/g++ /usr/bin/g++-4.8

# install specific orocksdb
RUN git clone https://github.com/domsj/orocksdb.git \
    && cd orocksdb \
    && git checkout tags/0.3.0 \
    && ./install_rocksdb.sh \
    && cp /usr/local/lib/librocksdb.so* /lib64/ \
    && sudo ldconfig -v


RUN pip install fabric junit-xml

# RUN wget http://cudf-solvers.irill.org/cudf_remote_proxy
# RUN chmod u+x cudf_remote_proxy
# RUN mv cudf_remote_proxy /usr/local/bin/

# ENV OPAMEXTERNALSOLVER="cudf_remote_proxy %{input}% %{output}% %{criteria}%"

RUN wget https://raw.github.com/ocaml/opam/master/shell/opam_installer.sh

env ocaml_version=4.04.2
RUN sh ./opam_installer.sh /usr/local/bin ${ocaml_version}

ENV opam_root=/home/jenkins/OPAM
ENV opam_env="opam config env --root=${opam_root}"
RUN opam init --root=${opam_root} --comp ${ocaml_version}

RUN eval `${opam_env}` && \
    opam update -v && \
    opam install -y \
        oasis.0.4.10 \
        ocamlfind \
        omake.0.10.2 \
        ssl.0.5.3 \
        camlbz2 \
        snappy \
        sexplib \
        bisect \
        lwt_ssl \
        lwt.3.0.0 \
        camltc.0.9.4 \
        ocplib-endian.1.0 \
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
        ppx_deriving.4.1 \
        ppx_deriving_yojson \
        base.v0.9.3 \
        core.v0.9.1 \
        redis.0.3.3 \
        uri.1.9.4 \
        result \
        ezxmlm


# AUTOMAKE-1.14.1 (for YASM)
RUN wget http://ftp.gnu.org/gnu/automake/automake-1.14.1.tar.xz \
        && tar -xvf automake-1.14.1.tar.xz \
        && cd automake-1.14.1 \
        && ./configure \
        && make \
        && make install

#YASM
RUN git clone --depth 1 git://github.com/yasm/yasm.git
RUN cd yasm && autoreconf -fiv && ./configure && make && make install && make distclean


RUN wget https://01.org/sites/default/files/downloads/intelr-storage-acceleration-library-open-source-version/isa-l-2.14.0.tar.gz
RUN tar xfzv isa-l-2.14.0.tar.gz
RUN cd isa-l-2.14.0 && ./configure
RUN cd isa-l-2.14.0 && make
RUN cd isa-l-2.14.0 && make install


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
    && cd googletest && git checkout release-1.8.0 \
    && mkdir build && cd build && cmake3 .. \
    && sudo make install
RUN cd googletest && git log --oneline | head -n 5

# libgtest.a ends up in /usr/local/lib/
RUN cp /usr/local/lib/libgtest.a /lib64/

# install specific arakoon.
RUN git clone https://github.com/openvstorage/arakoon.git
RUN cd arakoon && git pull && git checkout tags/1.9.22
RUN cd arakoon && eval `${opam_env}` && make
RUN cd arakoon && eval `${opam_env}` \
    && export PREFIX=${opam_root}/${ocaml_version} \
    && export OCAML_LIBDIR=`ocamlfind printconf destdir` \
    && make install


# install specific orocksdb
RUN eval `${opam_env}` \
    && cd orocksdb \
    && git checkout tags/0.3.0 \
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


RUN echo -e '[ovs]\nname=ovs\nbaseurl=http://yum.openvstorage.org/CentOS/7/x86_64/dists/unstable\nenabled=1\ngpgcheck=0' > /etc/yum.repos.d/ovs.repo
RUN cat /etc/yum.repos.d/ovs.repo
RUN yum -y update && \
    yum -y install iproute psmisc

## installing voldrv packages only works from within the OVS LAN

ARG INSTALL_VOLDRV_PACKAGES=false

RUN test ${INSTALL_VOLDRV_PACKAGES} = 'true' || echo 'we are NOT going to install the voldrv packages/testers'

# http://10.100.129.100:8080/view/volumedriver/view/centos/job/volumedriver-no-dedup-release-centos-7/35/artifact/volumedriver-core/build/rpm/volumedriver-no-dedup-base-debuginfo_6.10.1.0-1.rpm

ENV voldrv_jenkins=http://10.100.129.100:8080/view/volumedriver/view/centos/job/volumedriver-no-dedup-release-centos-7/35/artifact/volumedriver-core/build/rpm

ENV voldrv_base_pkg_name=volumedriver-no-dedup
ENV voldrv_version=6.10.1.0-1

RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-base_${voldrv_version}.rpm \
        && yum -y --nogpgcheck localinstall ${voldrv_base_pkg_name}-base_${voldrv_version}.rpm )
RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-pitreplication_${voldrv_version}.rpm \
        && yum -y --nogpgcheck localinstall ${voldrv_base_pkg_name}-pitreplication_${voldrv_version}.rpm )
RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-server_${voldrv_version}.rpm \
        && yum -y --nogpgcheck localinstall ${voldrv_base_pkg_name}-server_${voldrv_version}.rpm )
RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/libovsvolumedriver_${voldrv_version}.rpm \
        && yum -y --nogpgcheck localinstall libovsvolumedriver_${voldrv_version}.rpm )
RUN test ${INSTALL_VOLDRV_PACKAGES} = 'false' \
    || (wget ${voldrv_jenkins}/${voldrv_base_pkg_name}-test_${voldrv_version}.rpm \
        && yum -y --nogpgcheck localinstall ${voldrv_base_pkg_name}-test_${voldrv_version}.rpm )


RUN chmod ugoa+rxw -R ${opam_root}
RUN su - -c "echo 'eval `${opam_env}`' >> /home/jenkins/.bash_profile"
RUN su - -c "echo 'LD_LIBRARY_PATH=/usr/local/lib; export LD_LIBRARY_PATH;' >> /home/jenkins/.bash_profile"
RUN su - -c "echo 'VOLDRV_TEST=volumedriver_test; export VOLDRV_TEST;' >> /home/jenkins/.bash_profile"
RUN su - -c "echo 'VOLDRV_BACKEND_TEST=backend_test; export VOLDRV_BACKEND_TEST;' >> /home/jenkins/.bash_profile"

ENTRYPOINT ["/bin/bash", "-c", "set -e && /home/jenkins/alba/docker/docker-entrypoint.sh $@"]
