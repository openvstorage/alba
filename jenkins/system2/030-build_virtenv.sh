#!/bin/bash

set -e

virt="./_virt"

function install_virtenv {
    virtualenv $virt
    source $virt/bin/activate
}


if [ ! -d $virt ]; then
    echo "installing virtenv"
    install_virtenv
fi

. ./_virt/bin/activate
pip install fabric junit-xml
