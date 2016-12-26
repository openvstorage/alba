# Building alba (with docker)

### Clone the alba repo.

```
git clone https://github.com/openvstorage/alba
```

### Build and run the docker image

```
cd alba
./docker/run.sh ubuntu-16.04
```

then in the container

```
make
```

inspect the executable
```
./ocaml/alba.native version
```

run the (ocaml) unit tests:
```
./setup/setup.native ocaml
```

setup a demo env and play with it
```
./setup/setup.native nil
pgrep -a alba
pgrep -a arakoon
./ocaml/alba.native list-namespaces --config ./tmp/arakoon/abm.ini
Found the following namespaces: [("demo",
  { Albamgr_protocol.Protocol.Namespace.id = 0l; nsm_host_id = "nsm";
    state = Albamgr_protocol.Protocol.Namespace.Active;
    preset_name = "default" })
  ]
```

Now you have a mini alba installed inside a docker image. Have fun :)
