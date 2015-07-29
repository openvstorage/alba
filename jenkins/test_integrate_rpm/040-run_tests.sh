#!/bin/bash
set -e
set -v
. ./_virt/bin/activate

fab alba.rpm_integration_test:arakoon_version=$arakoon_version,alba_version=$alba_version,xml=True
