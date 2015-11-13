FROM ubuntu:14.04.3
RUN echo "deb http://archive.ubuntu.com/ubuntu/ trusty-backports main restricted universe multiverse" > /etc/apt/sources.list.d/trusty-backports.list
RUN apt-get -y update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential sudo python-dev python-pip wget gdebi-core
RUN pip install fabric junit-xml

RUN useradd jenkins -u 1001 -g root
RUN echo "jenkins ALL=NOPASSWD: ALL" >/etc/sudoers.d/jenkins
