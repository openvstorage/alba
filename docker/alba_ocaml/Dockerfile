FROM alba_base

ENV opam_env='opam config env --root=/home/alba/OPAM'
ENV alba_home=/home/alba/alba
ENV ARAKOON_BIN=arakoon

RUN apt-get install -y \
        libtool \
        libboost-all-dev \
        fuse
RUN wget http://ppa.launchpad.net/anatol/tup/ubuntu/pool/main/t/tup/tup_0.7.2.12+ga582fee_amd64.deb
RUN sudo dpkg -i tup_0.7.2.12+ga582fee_amd64.deb

# stuff below isn't really needed for jenkins.

CMD eval `${opam_env}` \
    && cd ${alba_home} \
    && git pull \
    && make \
    && fab dev.run_tests_ocaml

