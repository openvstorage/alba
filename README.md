Alba
====
[![Build Status](https://travis-ci.org/openvstorage/alba.svg?branch=master)](https://travis-ci.org/openvstorage/alba)

Alba (ALternative BAckend) is a distributed object store, like Swift, Ceph and Amplistor.
Alba is highly optimised for Open vStorage and aggregates a set of OSDs like Alba's ASDs or
[Seagate's Kinetic Drives](http://www.seagate.com/gb/en/products/enterprise-servers-storage/nearline-storage/kinetic-hdd/) into a pool of storage that you can use as a storage backend
for Open vStorage.

Alba supports the following high-end set of features:

* erasure coding
* replication
* (optional) compression
* (optional) encryption
* partial reads
* namespaces
* repair
* rebalancing
* ...


Unlike most alternatives, Alba is a consistent store.
Since it is optimised for the types of objects Open vStorage's volume driver needs to store and retrieve, Alba has become the default object storage backend for Open vStorage.

This also means Alba is not a general purpose object store.
Currently, you might have problems storing gigabyte size objects.


License
-------
ALBA is licensed under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

Building Alba
-------------
see [COMPILING](./COMPILING.md)

Playing Around
--------------
Developers can set up an alba environment on their local machine using the available fabric scripts:

```
$> fab alba.demo_kill alba.demo_setup
...
```


Alba's typically used from within a program (C++, OCaml) but there's a command line interface too.

```
$> ./ocaml/alba.native list-namespaces --config=cfg/test.ini
...
Found the following namespaces: [("demo",
  { Albamgr_protocol.Protocol.Namespace.id = 0l; nsm_host_id = "ricky";
    state = Albamgr_protocol.Protocol.Namespace.Active;
    preset_name = "default" })]
```

Uploading an object

```
$> ./ocaml/alba.native upload-object demo ./<some file> <OBJECT_NAME>
```

Downloading an object

```
$> ./ocaml/alba.native download-object demo <OBJECT_NAME> <the_file> --config=cfg/test.ini
```

You can administer a whole alba cluster via the command line but you probably want
to use OpenVStorage's GUI for this.


Architecture
------------
more information on design choices and how it all works is explained [here](./doc/architecture.pdf)
