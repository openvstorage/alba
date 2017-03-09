FROM ubuntu:16.04

RUN echo "deb http://apt.openvstorage.com unstable main" > /etc/apt/sources.list.d/ovsaptrepo.list

RUN apt-get -y update && apt-get install --allow-unauthenticated -y \
    sudo python-dev python-pip wget gdebi-core \
    libssl-dev libffi-dev
RUN pip install fabric junit-xml

RUN useradd jenkins -u 1500 -g root
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins

ARG INSTALL_VOLDRV_PACKAGES=false

ENTRYPOINT ["/bin/bash", "-c", "set -e && /home/jenkins/alba/docker/docker-entrypoint.sh $@"]
