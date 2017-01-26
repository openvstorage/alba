%global __os_install_post %{nil}%global __os_install_post %{nil}

Summary: Alba
Name: alba
Version: 1.3.5
Release: 1%{?dist}
License: Proprietary license
ExclusiveArch: x86_64

%description
Alba, a distributed storage system

%prep
cd ../SOURCES/alba
make clean

%build
cd ../SOURCES/alba
make

%install
cd ../SOURCES/alba
mkdir -p                               %{buildroot}%{_bindir}
cp ocaml/alba.native                   %{buildroot}%{_bindir}/alba
mkdir -p                               %{buildroot}%{_libdir}/alba/

for i in Jerasure rocksdb isal gf_complete; \
    do ldd ./ocaml/alba.native \
            | grep $i \
            | grep "=> /" \
            | awk '{print $3}' \
            | xargs -I{} cp -v "{}" %{buildroot}%{_libdir}/alba/ ;\
done

cp ocaml/albamgr_plugin.cmxs           %{buildroot}%{_libdir}/alba/
cp ocaml/nsm_host_plugin.cmxs          %{buildroot}%{_libdir}/alba/

%files
%attr(-, root, root) %{_bindir}/alba
%attr(-, root, root) %{_libdir}/alba/*

%post
echo %{_libdir}/alba/ > /etc/ld.so.conf.d/alba-x86_64.conf
/sbin/ldconfig

%postun
rm /etc/ld.so.conf.d/alba-x86_64.conf
/sbin/ldconfig

%changelog
* Sat Jan 21 2017 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 1.3.5
- Create Alba 1.3.5 RPM package
* Thu Jan 19 2017 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 1.3.4
- Create Alba 1.3.4 RPM package
* Tue Jan 10 2017 Jan Doms <jan.doms@gmail.com> - 1.3.3
- Create Alba 1.3.3 RPM package
* Tue Jan 03 2017 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 1.3.2
- Create Alba 1.3.2 RPM package
* Wed Dec 21 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 1.3.1
- Create Alba 1.3.1 RPM package
* Mon Dec 19 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 1.3.0
- Create Alba 1.3.0 RPM package
* Thu Dec 08 2016 Jan Doms <jan.doms@gmail.com> - 1.2.4
- Create Alba 1.2.4 RPM package
* Tue Dec 06 2016 Jan Doms <jan.doms@gmail.com> - 1.2.3
- Create Alba 1.2.3 RPM package
* Mon Dec 05 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 1.2.2
- Create Alba 1.2.2 RPM package
* Wed Nov 30 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 1.2.1
- Create Alba 1.2.1 RPM package
* Tue Nov 29 2016 Jan Doms <jan.doms@gmail.com> - 1.2.0
- Create Alba 1.2.0 RPM package
* Wed Nov 16 2016 Jan Doms <jan.doms@gmail.com> - 1.1.0
- Create Alba 1.1.0 RPM package
* Thu Nov 03 2016 Jan Doms <jan.doms@gmail.com> - 1.0.0
- Create Alba 1.0.0 RPM package
* Tue Oct 25 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.22
- Create Alba 0.9.22 RPM package
* Mon Oct 03 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.21
- Create Alba 0.9.21 RPM package
* Wed Sep 21 2016 Jan Doms <jan.doms@gmail.com> - 0.9.20
- Create Alba 0.9.20 RPM package
* Wed Aug 31 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.19
- Create Alba 0.9.19 RPM package
* Thu Aug 18 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.18
- Create Alba 0.9.18 RPM package
* Thu Aug 11 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.17
- Create Alba 0.9.17 RPM package
* Fri Aug 05 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.16
- Create Alba 0.9.16 RPM package
* Thu Aug 04 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.15
- Create Alba 0.9.15 RPM package
* Wed Jul 20 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.14
- Create Alba 0.9.14 RPM package
* Tue Jul 05 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.13
- Create Alba 0.9.13 RPM package
* Tue Jun 28 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.12
- Create Alba 0.9.12 RPM package
* Wed Jun 22 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.11
- Create Alba 0.9.11 RPM package
* Wed Jun 15 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.10
- Create Alba 0.9.10 RPM package
* Tue Jun 14 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.9
- Create Alba 0.9.9 RPM package
* Fri Jun 10 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.8
- Create Alba 0.9.8 RPM package
* Fri Jun 03 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.7
- Create Alba 0.9.7 RPM package
* Mon May 23 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.6
- Create Alba 0.9.6 RPM package
* Wed May 11 2016 Jan Doms <jan.doms@openvstorage.com> - 0.9.5
- Create Alba 0.9.5 RPM package
* Tue Apr 26 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.4
- Create Alba 0.9.4 RPM package
* Thu Apr 14 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.3
- Create Alba 0.9.3 RPM package
* Tue Mar 22 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.2
- Create Alba 0.9.2 RPM package
* Thu Mar 03 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.1
- Create Alba 0.9.1 RPM package
* Mon Feb 22 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.9.0
- Create Alba 0.9.0 RPM package
* Thu Feb 04 2016 Jan Doms <jan.doms@openvstorage.com> - 0.8.3
- Create Alba 0.8.3 RPM package
* Mon Jan 25 2016 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.8.2
- Create Alba 0.8.2 RPM package
* Thu Dec 24 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.8.1
- Create Alba 0.8.1 RPM package
* Mon Dec 14 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.8.0
- Create Alba 0.8.0 RPM package
* Wed Dec 02 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.7.4
- Create Alba 0.7.4 RPM package
* Thu Nov 26 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.7.3
- Create Alba 0.7.3 RPM package
* Wed Nov 18 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.30
- Create Alba 0.6.30 RPM package
* Tue Nov 10 2015 Jan Doms <jan.doms@openvstorage.com> - 0.7.2
- Create Alba 0.7.2 RPM package
* Tue Nov 10 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.29
- Create Alba 0.6.29 RPM package
* Fri Nov 06 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.28
- Create Alba 0.6.28 RPM package
* Fri Nov 06 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.27
- Create Alba 0.6.27 RPM package
* Fri Nov 06 2015 Jan Doms <jan.doms@openvstorage.com> - 0.7.1
- Create Alba 0.7.1 RPM package
* Thu Nov 05 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.26
- Create Alba 0.6.26 RPM package
* Tue Oct 27 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.25
- Create Alba 0.6.25 RPM package
* Mon Oct 19 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.24
- Create Alba 0.6.24 RPM package
* Wed Oct 14 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.23
- Create Alba 0.6.23 RPM package
* Fri Oct 09 2015 Romain Slootmaekers <romain.slootmaekers@cloudfounders.com> - 0.6.22
- Create Alba 0.6.22 RPM package
* Fri Oct 02 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.21
- Create Alba 0.6.21 RPM package
* Tue Sep 29 2015 Romain Slootmaekers <romain.slootmaekers@cloudfounders.com> - 0.7.0
- Create Alba 0.7.0 RPM package
* Tue Sep 22 2015 Romain Slootmaekers <romain.slootmaekers@cloudfounders.com> - 0.6.20
- Create Alba 0.6.20 RPM package
* Wed Sep 16 2015 Romain Slootmaekers <romain.slootmaekers@cloudfounders.com> - 0.6.19
- Create Alba 0.6.19 RPM package
* Wed Sep 09 2015 Romain Slootmaekers <romain.slootmaekers@cloudfounders.com> - 0.6.18
- Create Alba 0.6.18 RPM package
* Tue Sep 08 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.17
- Create Alba 0.6.17 RPM package
* Fri Sep 04 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.16
- Create Alba 0.6.16 RPM package
* Wed Sep 02 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.15
- Create Alba 0.6.15 RPM package
* Mon Aug 31 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.14
- Create Alba 0.6.14 RPM package
* Thu Aug 27 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.13
- Create Alba 0.6.13 RPM package
* Tue Aug 25 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.12
- Create Alba 0.6.12 RPM package
* Mon Aug 24 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.11
- Create Alba 0.6.11 RPM package
* Thu Aug 20 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.10
- Create Alba 0.6.10 RPM package
* Wed Aug 12 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.9
- Create Alba 0.6.9 RPM package
* Fri Aug 07 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.8
- Create Alba 0.6.8 RPM package
* Wed Aug 05 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.7
- Create Alba 0.6.7 RPM package
* Tue Aug 04 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.6
- Create Alba 0.6.6 RPM package
* Mon Aug 03 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.5
- Create Alba 0.6.5 RPM package
* Thu Jul 30 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.4
- Create Alba 0.6.4 RPM package
* Fri Jul 17 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.6.3
- Create Alba 0.6.3 RPM package
* Wed Jul 15 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.2
- Create Alba 0.6.2 RPM package
* Mon Jul 13 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.1
- Create Alba 0.6.1 RPM package
* Wed Jul 08 2015 Jan Doms <jan.doms@openvstorage.com> - 0.6.0
- Create Alba 0.6.0 RPM package
* Fri Jul 03 2015 Jan Doms <jan.doms@openvstorage.com> - 0.5.14
- Create Alba 0.5.14 RPM package
* Thu Jul 02 2015 Jan Doms <jan.doms@openvstorage.com> - 0.5.13
- Create Alba 0.5.13 RPM package
* Fri Jun 26 2015 Romain Slootmaekers <romain.slootmaekers@openvstorage.com> - 0.5.12
- Create Alba 0.5.12 RPM package
* Wed Jun 24 2015 Jan Doms <jan.doms@openvstorage.com> - 0.5.11
- Create alba 0.5.11 RPM package
* Tue Jun 23 2015 Jan Doms <jan.doms@openvstorage.com> - 0.5.10
- Create alba 0.5.10 RPM package
* Wed Jun 17 2015 Jan Doms <jan.doms@openvstorage.com> - 0.5.9
- Create alba 0.5.9 RPM package
* Thu Jun 11 2015 Jan Doms <jan.doms@openvstorage.com> - 0.5.8
- Create alba 0.5.8 RPM package
