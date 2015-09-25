from alba_debian
ENV opam_env='opam config env --root=/home/alba/OPAM'
ENV ARAKOON_BIN=arakoon
CMD eval `${opam_env}` \
    && echo ${SUITE} \
    && ./jenkins/run.sh ${SUITE}
