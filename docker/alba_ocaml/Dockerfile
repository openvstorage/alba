FROM alba_base

RUN apt-get install -y strace

ENV opam_env='opam config env --root=/home/alba/OPAM'
ENV alba_home=/home/alba/alba
ENV ARAKOON_BIN=arakoon
CMD eval `${opam_env}` \
    && cd ${alba_home} \
    && git pull \
    && make \
    && fab dev.run_tests_ocaml

