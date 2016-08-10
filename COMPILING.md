# Building alba (with docker)

### Clone the alba repo.

```
git clone https://github.com/openvstorage/alba
```

### Build the docker image

```
cd alba
sudo docker build --rm=true --tag=alba_ubuntu_16_04 ./docker/alba_ubuntu_16_04/
```

### Run the docker image
```
sudo docker run -i -t -e UID=${UID} -v ${PWD}:/home/jenkins/alba -w /home/jenkins/alba alba_ubuntu_16_04 bash -l
```

then in the container

```
cd alba/
make
```

inspect the executable
```
./ocaml/alba.native version
```

run the (ocaml) unit tests:
```
ARAKOON_BIN=~/OPAM/4.02.3/bin/arakoon ./setup/setup.native ocaml
```

setup a demo env and play with it
```
ARAKOON_BIN=~/OPAM/4.02.3/bin/arakoon ./setup/setup.native nil
pgrep -a alba
pgrep -a arakoon
./ocaml/alba.native list-namespaces --config cfg/test.ini
Found the following namespaces: [("demo",
  { Albamgr_protocol.Protocol.Namespace.id = 0l; nsm_host_id = "ricky";
    state = Albamgr_protocol.Protocol.Namespace.Active;
    preset_name = "default" })]
```

Now you have a mini alba installed inside a docker image. Have fun :)
