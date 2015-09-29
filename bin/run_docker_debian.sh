ALBA_HOME=/home/romain/workspace/ALBA/alba
INSIDE=/home/jenkins/workspace/the_job/
docker run --rm=true -v \
       ${ALBA_HOME}:${INSIDE} \
       --env "SUITE=$1" \
       -w ${INSIDE} alba_debian_jenkins
