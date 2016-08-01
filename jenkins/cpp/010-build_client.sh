#!/bin/bash

set -v
set -e
g++ --version
cd ./cpp/automake_client/
./build.sh
./configure
make clean
make

cd ../

#CXX=clang++-3.5 make clean all

CXX=g++ make clean all
