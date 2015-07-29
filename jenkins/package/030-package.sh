#!/bin/bash

export KINETIC=${PWD}/../kinetic-cpp-client
export LD_LIBRARY_PATH=$KINETIC:$KINETIC/lib:$KINETIC/vendor/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/cpp/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib

./package.py
