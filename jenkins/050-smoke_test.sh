##!/usr/bin/env bash

set -e
set -v
source ./_virt/bin/activate

fab alba.smoke_test

pkill alba.native
pkill arakoon
