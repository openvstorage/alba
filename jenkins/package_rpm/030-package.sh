#!/bin/bash

eval `${opam_env}`
export START=${PWD}
echo START=${START}
mkdir -p ${START}/rpmbuild/SOURCES
cd ${START}/rpmbuild/SOURCES/
ln -s ${START} ./alba
cd ${START}
chown root:root ./redhat/SPECS/alba.spec
rpmbuild --define "_topdir ${START}/rpmbuild" -bb ./redhat/SPECS/alba.spec
