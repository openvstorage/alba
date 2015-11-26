#!/bin/bash

set -v
set -e

cd ./cpp/automake_client/
./build.sh
./configure
make clean
make

cd ../

CXX=clang++-3.5 make
