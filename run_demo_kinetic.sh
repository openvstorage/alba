export ALBA_IP=172.17.0.1
export ALBA_ASD_LOG_LEVEL=debug
export ALBA_KINETIC=true

function setup {
    pkill alba
    pkill arakoon
    pkill java
    ./setup/setup.native nil

    ./ocaml/alba.native create-preset \
                        --config ${WORKSPACE}/tmp/arakoon/abm.ini \
                        preset_kinetic < ./cfg/preset_kinetic.json

    export namespace=kinetic_demo
    ./ocaml/alba.native create-namespace ${namespace} preset_kinetic \
                        --config ${WORKSPACE}/tmp/arakoon/abm.ini

}

export ALBA_PROXY_IP=${ALBA_IP}

function bench {
    ./ocaml/alba.native proxy-bench \
                        -h ${ALBA_IP} -p 10000 \
                        -n 100 \
                        --file /home/romain/workspace/ALBA/alba/setup/setup.ml \
                        ${namespace}
}

setup
bench
wait
