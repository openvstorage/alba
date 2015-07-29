#!/bin/bash
set -e
. ./_virt/bin/activate

# archive the artifacts
mkdir -p ~/archives
tar -czvf ~/archives/$BUILD_TAG.tar.gz -C /tmp . || true
sudo rm -rf /tmp/*

fab alba.smoke_test
