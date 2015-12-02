##!/usr/bin/env bash

set -e
set -v

if [ $tls == "true" ]
then
    tls_s=True
else
    tls_s=False
fi
fab dev.run_tests_ocaml:xml=True,tls=${tls_s} || true
