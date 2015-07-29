#!/usr/bin/env bash

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 2 ] || die "2 arguments required, $# provided"
echo $1 | grep -E -q '^[0-9]+$' || die "Numeric argument required, $1 provided"


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
root=${DIR%%/bin}
bin_dir=${root}/bin

EXEC=./startSimulator.sh
export JAVA_HOME=/usr/lib/jvm/default-java
let tlsport=$((1000 + $1))
echo "port=$1"
echo "home=$2"
echo "tlsport=$tlsport"
echo ${EXEC}

cd ${bin_dir}
${EXEC} -port $1 -tlsPort $tlsport -home $2
