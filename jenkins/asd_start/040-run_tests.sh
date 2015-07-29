#!/bin/bash
set -e
set -v
. ./_virt/bin/activate

fab dev.run_test_asd_start:xml=True || true
