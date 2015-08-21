#!/bin/bash

set -v
set -e

cd ./cpp/automake_client/
./build.sh
./configure
make clean
make

cd ../
if [ ! -d ".tup" ]; then
    tup init
    printf 'CONFIG_COMPILER=clang++-3.5\n' > ./tup.config

fi

rm -f $(find ./ -name "*.o")
rm -f lib/*
rm -f bin/*

tup
