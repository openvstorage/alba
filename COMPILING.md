# Alba Development Setup

### Clone the alba repo.

```
git clone https://github.com/openvstorage/alba
```

### Install the dependencies.
The exact, up-to-date list of what you need can be found in the dockerfiles we
use for CI.

*    [ubuntu/debian](./docker/alba_debian_jenkins/Dockerfile)
*    [centos7](./docker/alba_centos7_jenkins/Dockerfile)

### compile Alba
```
make
```

inspect executable

```
$> ./ocaml/alba.native version
```

### setup a local alba

- explain where the setup code can find your arakoon

```
export ARAKOON_BIN=$(which arakoon)
```

- make a demo environment
```
$> ./setup/setup.native nil
```

- list the namespaces

```
$> ./ocaml/alba.native list-namespaces --config cfg/test.ini
Found the following namespaces: [("demo",
  { Albamgr_protocol.Protocol.Namespace.id = 0l; nsm_host_id = "ricky";
    state = Albamgr_protocol.Protocol.Namespace.Active;
    preset_name = "default" })]
```

Now you have a mini alba installed on your machine. Have fun :)
