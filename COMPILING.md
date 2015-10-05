# Alba Development Setup

### Clone the alba repo.

```
git clone https://github.com/openvstorage/alba
```

### Install the dependencies.
The exact, up-to-date list of what you need can be found in the dockerfiles we
use for CI.

*    [ubuntu/debian](./docker/alba_debian/Dockerfile).
*    [centos7](./docker/alba_centos7/Dockerfile)

### compile Alba
```
make
```

inspect executable

```
./ocaml/alba.native version
```


### setup a local alba

- explain where fabric can find your arakoon

```
export ARAKOON_BIN=$(which arakoon)
```

- make a demo environment
```
$> fab alba.demo_kill alba.demo_setup
```

- list the namespaces

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

Now you have a mini alba installed on your machine. Have fun :)
