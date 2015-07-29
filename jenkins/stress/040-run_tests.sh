#!/bin/bash
set -e
set -v
. ./_virt/bin/activate

fab dev.run_tests_stress:xml=True || true
