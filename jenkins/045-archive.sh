#!/usr/bin/env bash

# archive the artifacts
mkdir -p ~/archives
tar -czvf ~/archives/$BUILD_TAG.tar.gz -C /tmp . || true
sudo rm -rf /tmp/*
