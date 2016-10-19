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
created_package=`ls -t rpmbuild/RPMS/x86_64/alba-*.rpm | head -n1`
new_package=alba-`git describe --tags --dirty | xargs`-1.el7.centos.x86_64.rpm
mv ${created_package} ${new_package}
