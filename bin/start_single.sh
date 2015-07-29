die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided"
echo $1 | grep -E -q '^[0-9]+$' || die "Numeric argument required, $1 provided"

EXEC=./startSimulator.sh
export JAVA_HOME=/usr/lib/jvm/default-java
khome=$HOME/kinetic/$1
let tlsport=$((1000 + $1))
echo $tlsport
${EXEC} -port $1 -tlsPort $tlsport -home $khome
