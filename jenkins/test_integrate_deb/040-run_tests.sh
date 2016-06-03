#!/bin/bash
set -e
set -v

find cfg/*.ini -exec sed -i "s,/tmp,${WORKSPACE}/tmp,g" {} \;
fab alba.deb_integration_test:arakoon_url=$arakoon_url,alba_url=$alba_url,xml=True
