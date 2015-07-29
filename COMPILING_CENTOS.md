NOTE: These instructions are not complete, they only highlight the differences with
the ubuntu compiling instructions.

Install EPEL ( https://fedoraproject.org/wiki/EPEL )
> sudo yum install epel-release

Install yasm
> sudo yum install yasm automake

Install jerasure
> wget http://people.redhat.com/zaitcev/tmp/jerasure-2.0-1.fc20.src.rpm
> rpm -i jerasure-2.0-1.fc20.src.rpm

Install automake 1.14.1
> wget http://ftp.gnu.org/gnu/automake/automake-1.14.1.tar.gz
> tar xfzv automake-1.14.1.tar.gz
> cd automake-1.14.1
> ./configure
> make
> make install
> sudo make install

Now ISA-L can be installed; see instructions for ubuntu.
