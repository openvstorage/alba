##!/usr/bin/env bash

set -e
set -v
source ./_virt/bin/activate

fab alba.smoke_test

pkill alba.native
pkill arakoon

# archive the artifacts
mkdir -p ~/archives
tar -czvf ~/archives/$BUILD_TAG.tar.gz -C /tmp . || true

sudo rm -rf /tmp/*
