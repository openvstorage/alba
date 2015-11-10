#!/bin/bash

set -e

make clean
opam switch
make
