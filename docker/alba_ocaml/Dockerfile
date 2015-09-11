FROM alba_base

ENV opam_env='opam config env --root=/home/alba/OPAM'
ENV alba_home=/home/alba/alba
ENV ARAKOON_BIN=arakoon

RUN apt-get install -y libtool

# stuff below isn't really needed for jenkins.

CMD eval `${opam_env}` \
    && cd ${alba_home} \
    && git pull \
    && make \
    && fab dev.run_tests_ocaml

