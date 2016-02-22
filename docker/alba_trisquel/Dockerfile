FROM kpengboy/trisquel:latest
RUN apt-get update && apt-get install -y wget libsnappy1 libev4 libffi6 libgflags2 sudo libgcrypt20
RUN wget \
        https://github.com/openvstorage/arakoon/releases/download/1.9.1/arakoon_1.9.1_amd64.deb \
        https://github.com/openvstorage/alba/releases/download/0.9.0/alba_0.9.0_amd64.deb
RUN dpkg -i arakoon_1.9.1_amd64.deb alba_0.9.0_amd64.deb


