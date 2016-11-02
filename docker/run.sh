#!/bin/bash -xue

IMAGE=$1
shift

docker build --rm=true \
       --tag=alba_$IMAGE \
       --build-arg INSTALL_VOLDRV_PACKAGES=${INSTALL_VOLDRV_PACKAGES:-false} \
       ./docker/$IMAGE \


if [ -t 1 ];
then TTY="-t";
else TTY="";
fi

docker run -i $TTY --privileged=true -e UID=${UID} \
       --env ALBA_TLS --env ALBA_USE_ETCD --env ALBA_USE_GIOEXECFILE \
       --env arakoon_url --env alba_url \
       -v ${PWD}:/home/jenkins/alba \
       alba_$IMAGE \
       bash $@
