FROM ubuntu:15.04
RUN apt-get update
RUN apt-get install -y wget

RUN wget https://github.com/openvstorage/arakoon/releases/download/1.9.1/arakoon_1.9.1_amd64.deb
RUN apt-get install -y libsnappy1 libev4
RUN dpkg -i arakoon_1.9.1_amd64.deb

RUN wget https://github.com/openvstorage/alba/releases/download/0.9.0/alba_0.9.0_amd64.deb
RUN apt-get install -y libffi6 libgflags2 sudo
RUN sudo dpkg -i alba_0.9.0_amd64.deb


