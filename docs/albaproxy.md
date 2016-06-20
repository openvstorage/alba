# ALBA Proxy
The ALBA proxy sits between the [Volume Driver](https://github.com/openvstorage/volumedriver) and the [ALBA backend](README.md). It runs as a process on the Storage Router host and takes the SCOs coming from the Volume Driver and stores them according to a policy on the different OSDs of the backend.
There is an ALBA Proxy per vPool which is available on the Storage Router. The port on which a vPool is listening for a certain vPool can be found in the config file of the proxy (`/opt/OpenvStorage/config/storagedriver/storagedriver/<vpool_name>_alba.json`).


## ALBA Proxy Config files
There is an ALBA Proxy configuration file per vPool. The config file is located at `/opt/OpenvStorage/config/storagedriver/storagedriver/<vpool_name>_alba.json`. More info on the proxy config files can be found [here](../../Administration/Configs/albaproxy.md)

## ALBA Proxy Log files
The log files for the ALBA proxy can be found under `/var/log/upstart/ovs-albaproxy_<vpool_name>.log`.

## Basic commands
### List all ALBA proxies
All are running as a service with as name `ovs-albaproxy_<vpool_name>`.

```
ovs monitor services

...
ovs-albaproxy_poc01-vpool-ovs start/running, process 16268

```
In this case we have a proxy for the vPool `poc01-vpool-ovs`.


### Start a proxy
To start the proxy:

```
start ovs-albaproxy_<vpool_name>
```

### Restart a proxy
To restart a proxy:

```
restart ovs-albaproxy_<vpool_name>
```

### Stop a proxy
To stop a proxy
```
stop ovs-albaproxy_<vpool_name>
```

### Get the version of the ALBA proxy
To see the version of the proxy running, execute in the shell:
```
alba proxy-get-version -h 127.0.0.1 -p 26203
    (0, 7, 3, "0.7.3-0-gd2ea678")
```

### List all namespaces registered on the ALBA proxy
To list all namespaces, execute in the shell:
```
root@cmp02:~# alba proxy-list-namespaces -h 127.0.0.1 -p 26203
Found 3 namespaces: ["17c2b966-4e52-40fa-9f87-0d241a14157d";
 "dafa3036-d47e-4ebf-ba65-0cf46a0a9a1b";
 "fd-poc01-vpool-ovs-4d8fa3f7-3632-41db-b18f-6ecbe4782c51"]
```

### List objects of a namespace
To list the objects of a namespace, execute in the shell
```
root@cmp02:~# alba proxy-list-objects '183a2397-453a-4377-9537-53d591cd2a37'  -h 127.0.0.1 -p 26203
((100,
  ["00_00000001_00"; "00_00000002_00"; "00_00000003_00"; "00_00000004_00";
   "00_00000005_00"; "00_00000006_00"; "00_00000007_00"; "00_00000008_00";
   "00_00000009_00"; "00_0000000a_00"; "00_0000000b_00"; "00_0000000c_00";
   "00_0000000d_00"; "00_0000000e_00"; "00_0000000f_00"; "00_00000010_00";
   "00_00000011_00"; "00_00000012_00"; "00_00000013_00"; "00_00000014_00";
   "00_00000015_00"; "00_00000016_00"; "00_00000017_00"; "00_00000018_00";
   "00_00000019_00"; "00_0000001a_00"; "00_0000001b_00"; "00_0000001c_00";
   "00_0000001d_00"; "00_0000001e_00"; "00_0000001f_00"; "00_00000020_00";
   "00_00000021_00"; "00_00000022_00"; "00_00000023_00"; "00_00000024_00";
   "00_00000025_00"; "00_00000026_00"; "00_00000027_00"; "00_00000028_00";
   "00_00000029_00"; "00_0000002a_00"; "00_0000002b_00"; "00_0000002c_00";
   "00_0000002d_00"; "00_0000002e_00"; "00_0000002f_00"; "00_00000030_00";
   "00_00000031_00"; "00_00000032_00"; "00_00000033_00"; "00_00000034_00";
   "00_00000035_00"; "00_00000036_00"; "00_00000037_00"; "00_00000038_00";
   "00_00000039_00"; "00_0000003a_00"; "00_0000003b_00"; "00_0000003c_00";
   "00_0000003d_00"; "00_0000003e_00"; "00_0000003f_00"; "00_00000040_00";
   "00_00000041_00"; "00_00000042_00"; "00_00000043_00"; "00_00000044_00";
   "00_00000045_00"; "00_00000046_00"; "00_00000047_00"; "00_00000048_00";
   "00_00000049_00"; "00_0000004a_00"; "00_0000004b_00"; "00_0000004c_00";
   "00_0000004d_00"; "00_0000004e_00"; "00_0000004f_00"; "00_00000050_00";
   "00_00000051_00"; "00_00000052_00"; "00_00000053_00"; "00_00000054_00";
   "00_00000055_00"; "00_00000056_00"; "00_00000057_00"; "00_00000058_00";
   "00_00000059_00"; "00_0000005a_00"; "00_0000005b_00"; "00_0000005c_00";
   "00_0000005d_00"; "00_0000005e_00"; "00_0000005f_00"; "00_00000060_00";
   "00_00000061_00"; "00_00000062_00"; "00_00000063_00"; "00_00000064_00"]),
 true)
```

By default the first 100 objects will be returned. By passing additional arguments you can browse through all objects in the namespace.

### Upload an object to a namespace
To upload an object to a namespace, execute in the shell
```
root@cmp02:~# alba proxy-list-objects '<namespace>' '</path/to/file>' '<key in ALBA>'  -h 127.0.0.1 -p 26203
```

## Use the OVS client to manage an ALBA proxy
The OVS python client allow to manage an ALBA proxy. In the below example we retrieve the proxy version.
```
from ovs.extensions.plugins.albacli import AlbaCLI
AlbaCLI.run('proxy-get-version', extra_params=['-h 127.0.0.1', '-p 2620
```
The output is the Proxy version:
```
'(0, 7, 3, "0.7.3-0-gd2ea678")'
```