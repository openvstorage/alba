#!/bin/bash

# this script is executed at each startup of the container
# NOTE: for jenkins, make sure to trigger a rebuild of the docker image 
#       when making changes to this file! (md5sum of Dockerfile should change)

set -e
set -x

# hack to make sure we have access to files in the jenkins home directory 
# the UID of jenkins in the container should match our UID on the host
if [ ${UID} -ne 1001 ]
then
    sed -i "s/x:1001:/x:${UID}:/" /etc/passwd
    chown ${UID} /home/jenkins 
    [ -d /home/jenkins/.ssh ] && chown ${UID} /home/jenkins/.ssh
fi

# finally execute the command the user requested
exec "$@"
