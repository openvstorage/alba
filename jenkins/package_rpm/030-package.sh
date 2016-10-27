#!/bin/bash -xue

eval `${opam_env}`
export START=${PWD}
echo START=${START}
rm -f ./rpmbuild/SOURCES/alba
mkdir -p ${START}/rpmbuild/SOURCES

cd ${START}/rpmbuild/SOURCES/
ln -s ${START} ./alba
cd ${START}
sudo chown root:root ./redhat/SPECS/alba.spec
rpmbuild --define "_topdir ${START}/rpmbuild" -bb ./redhat/SPECS/alba.spec

RPMS=/home/jenkins/alba/rpmbuild/RPMS/x86_64

created_package=`ls -t ${RPMS}/alba-*.centos.x86_64.rpm | head -n1`
new_package=${RPMS}/alba-`git describe --tags --dirty | xargs`-1.el7.centos7.x86_64.rpm
echo $created_package
echo $new_package
if [$created_package == $new_package]
then echo "point release"
else mv ${created_package} ${new_package}
fi
