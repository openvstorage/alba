%global __os_install_post %{nil}%global __os_install_post %{nil}

Summary: Alba
Name: alba
Version: 0.6.3
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
cp ocaml/albamgr_plugin.cmxs           %{buildroot}%{_libdir}/alba/
cp ocaml/nsm_host_plugin.cmxs          %{buildroot}%{_libdir}/alba/
cp /usr/local/lib/libJerasure.so.2     %{buildroot}%{_libdir}/alba/
cp /usr/local/lib/librocksdb.so	       %{buildroot}%{_libdir}/alba/
cp /usr/local/lib/libgf_complete.so.1  %{buildroot}%{_libdir}/alba/


%files
%{_bindir}/alba
%{_libdir}/alba/*

%post
echo %{_libdir}/alba/ > /etc/ld.so.conf.d/alba-x86_64.conf
/sbin/ldconfig

%postun
rm /etc/ld.so.conf.d/alba-x86_64.conf
/sbin/ldconfig

%changelog
* Fri Jul 17 2015 Romain Slootmaekers <romain.slootmaekers@cloudfounders.com> - 0.6.3
- Create Alba 0.6.3 RPM package
* Wed Jul 15 2015 Jan Doms <jan.doms@gmail.com> - 0.6.2
- Create Alba 0.6.2 RPM package
* Mon Jul 13 2015 Jan Doms <jan.doms@gmail.com> - 0.6.1
- Create Alba 0.6.1 RPM package
* Wed Jul 08 2015 Jan Doms <jan.doms@gmail.com> - 0.6.0
- Create Alba 0.6.0 RPM package
* Fri Jul 03 2015 Jan Doms <jan.doms@gmail.com> - 0.5.14
- Create Alba 0.5.14 RPM package
* Thu Jul 02 2015 Jan Doms <jan.doms@gmail.com> - 0.5.13
- Create Alba 0.5.13 RPM package
* Fri Jun 26 2015 Romain Slootmaekers <romain.slootmaekers@cloudfounders.com> - 0.5.12
- Create Alba 0.5.12 RPM package
* Wed Jun 24 2015 Jan Doms <jan.doms@cloudfounders.com> - 0.5.11
- Create alba 0.5.11 RPM package
* Tue Jun 23 2015 Jan Doms <jan.doms@cloudfounders.com> - 0.5.10
- Create alba 0.5.10 RPM package
* Tue Jun 17 2015 Jan Doms <jan.doms@cloudfounders.com> - 0.5.9
- Create alba 0.5.9 RPM package
* Thu Jun 11 2015 Jan Doms <jan.doms@gmail.com> - 0.5.8
- Create alba 0.5.8 RPM package
