#!/bin/bash

eval `opam config env`

mkdir ./redhat/SOURCES
ln -s '../../' ./redhat/SOURCES/alba | true
ALBA_HOME=`pwd`
cd
rm rpmbuild | true
ln -s $ALBA_HOME/redhat rpmbuild
cd rpmbuild
rpmbuild -ba SPECS/alba.spec
