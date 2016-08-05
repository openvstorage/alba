#!/bin/bash -xue

IMAGE=$1
shift

docker build --rm=true --tag=alba_$IMAGE ./docker/$IMAGE

if [ -t 1 ];
then TTY="-t";
else
    # this path is taken on jenkins, clean previous builds first
    TTY="";
    docker run -i $TTY --privileged=true -e UID=${UID} \
           -v ${PWD}:/home/jenkins/alba \
           -w /home/jenkins/alba alba_$IMAGE \
           bash -l -c "cd alba && ./jenkins/run2.sh clean"
fi

docker run -i $TTY --privileged=true -e UID=${UID} \
       --env ALBA_TLS --env ALBA_USE_ETCD --env ALBA_USE_GIOEXECFILE \
       -v ${PWD}:/home/jenkins/alba \
       -w /home/jenkins/alba alba_$IMAGE \
       bash -l -c "cd alba && ./jenkins/run2.sh $@"
