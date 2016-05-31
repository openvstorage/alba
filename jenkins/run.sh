#!/bin/bash -xue

env | sort

export WORKSPACE=$PWD
echo ${WORKSPACE}

for f in $(ls ./jenkins/$1);
do
   echo $f
   ./jenkins/$1/$f
done;
