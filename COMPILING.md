# Alba Installation

> Clone the alba repo.
>> - git clone https://github.com/openvstorage/alba

> Install the dependencies.
>> - sudo apt-get install libprotoc-dev protobuf-compiler libjerasure-dev yasm

> Install ISA-L v2.14
>> Download the package from https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version/downloads .
>> ./configure
>> make
>> sudo make install

> List of required opam packages (See the file _tags under ocaml directory. You don't have to install all these package because some of them will be installed automatically during arakoon installation.)
>> arakoon.1.8.8
>> kinetic-client
>> tiny_json
>> cmdliner
>> ppx_deriving_yojson
>> lwt.2.5.0
>> oUnit
>> ctypes
>> ctypes-foreign
>> snappy
>> uuidm
>> orocksdb

>Run make in alba home directory.
>> - make

> If any package is still missing then `make` will show the error message about missing package.
>> - ocamlfind ocamldep -package kinetic-client -package nocrypto -package uuidm -package tiny_json -package snappy.

>> - ocamlfind: Package `kinetic-client' not found

>> - Command exited with code 2.
Compilation unsuccessful after building 1 target (0 cached) in 00:00:00.

>To install the packages
>> - opam install package-name

>During the installation, you may be asked to upgrade a package. It's always preferable to pin the package (don't upgrade) specified by developer.
>> - opam pin package-name version


> If all the required packages are installed then `make` will run without error. Run the below command in alba home directory. We recommend to use kernel >= 3.15 because of bug in kernel from 3.10 to 3.14 which impacts the performance of asds.

```
$> ./ocaml/alba.native version
-1.-1.-1
git_revision: "heads/proxy_bench-0-gc6ea5f7"
git_repo: "https://github.com/openvstorage/alba.git"
compile_time: "18/08/2015 12:43:11 UTC"
machine: "nsanga2 3.19.0-26-generic x86_64 x86_64 x86_64 GNU/Linux"
compiler_version: "4.02.1"
dependencies:
arakoon_client      	           1.8.8	Arakoon client
bz2                 	           0.6.0	[n/a]
cmdliner            	           0.9.7	Declarative definition of command line interfaces
cstruct             	           1.7.0	Manipulate external buffers as C-like structs
ctypes              	           0.4.1	Combinators for binding to C libraries without writing any C.
ctypes.foreign      	           0.4.1	Dynamic linking of C functions
kinetic-client      	           0.0.6	Kinetic client
lwt                 	           2.5.0	Lightweight thread library for OCaml (core library)
lwt.unix            	           2.5.0	Unix support for lwt
oUnit               	           2.0.0	Unit testing framework
ocplib-endian       	             0.8	Optimised functions to read and write int16/32/64 from strings and bigarrays
ppx_deriving.enum   	             2.2	[@@deriving enum]
ppx_deriving.show   	             2.2	[@@deriving show]
ppx_deriving_yojson 	             2.3	[@@deriving yojson]
rocks               	           0.2.1	Rocksdb binding
sexplib             	       112.35.00	sexplib - automated S-expression conversion
snappy              	           0.1.0	Bindings to snappy compression library
ssl                 	           0.5.0	OCaml bindings to libssl
tiny_json           	           1.1.2	A small Json library from OCAMLTTER
yojson              	           1.2.1	JSON parsing and printing (successor of json-wheel)
```


> Install fabric and junit-xml
>> - sudo pip install fabric junit-xml

>Create symbolic link from arakoon installed directory to /home/your-user-name/workspace/ARAKOON/arakoon/ (The name of directory should be workspace, ARAKOON and arakoon as it is).
>> - ln -s /home/mukeshtiwari/Programming/cloudfounder/arakoon/arakoon.native  /home/mukeshtiwari/workspace/ARAKOON/arakoon/


> Install an alba demo setup or kill existing setup and reinstall a clean instance.

```
$> fab alba.demo_kill alba.demo_setup
```

> If everything is great so far then run

```
$> ocaml/alba.native list-namespaces --config cfg/test.ini

Jul  7 16:21:11.0773 main debug: Taking an albamgr from the connection pool
Jul  7 16:21:11.0776 main debug: Client_main.find_master': Trying "arakoon_0"
Jul  7 16:21:11.0782 main debug: Client_main.find_master': "arakoon_0" thinks "arakoon_0"
Jul  7 16:21:11.0782 main debug: Client_main.find_master': Found arakoon_0 (ips = 127.0.0.1, port = 4000)
Jul  7 16:21:11.0801 main debug: user hook was found
Jul  7 16:21:11.0804 main debug: Got an albamgr from the connection pool
Jul  7 16:21:11.0804 main debug: albamgr_client: ListNamespaces
Found the following namespaces: [("demo",
  { Albamgr_protocol.Protocol.Namespace.id = 0l; nsm_host_id = "ricky";
    state = Albamgr_protocol.Protocol.Namespace.Active;
    preset_name = "default" })]
```

> Now you have a mini alba installed on your machine. Have fun :)
