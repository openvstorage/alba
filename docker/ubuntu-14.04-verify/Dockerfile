FROM ubuntu:14.04.3
RUN echo "deb http://archive.ubuntu.com/ubuntu/ trusty-backports main restricted universe multiverse" > /etc/apt/sources.list.d/trusty-backports.list

RUN echo "deb http://apt.openvstorage.org unstable main" > /etc/apt/sources.list.d/ovsaptrepo.list

RUN apt-get -y update && DEBIAN_FRONTEND=noninteractive apt-get install --force-yes -y \
    build-essential sudo python-dev python-pip wget gdebi-core
RUN pip install fabric junit-xml

RUN useradd jenkins -u 1500 -g root
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins

ARG INSTALL_VOLDRV_PACKAGES=false

ENTRYPOINT ["/bin/bash", "-c", "set -e && /home/jenkins/alba/docker/docker-entrypoint.sh $@"]
