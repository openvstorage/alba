# Self Healing
ALBA has built-in self healing functionality. In case an ASD is broken, fragments on the ASD will be automatically reconstructed on remaining disks in the Backend.

The self healing work consists out of several types of maintenance work:

1. Message delivery (from albamgr to osds & nsm hosts)
2. Check if available osds are claimed by another env
3. Rebalancing data (when new nodes/disks are added)
4. Repairing decommissioned disks
5. Cleaning up obsolete fragments
6. Garbage collecting fragments written by dead (or extremely slow) clients
7. Repair by policy (upgrade objects written with a narrow policy to wider policies when applicable)
8. Diverse work items (cleanup osds/namespaces, repair object for which a bad fragment was detected, rewrite a namespace, ...)
9. Cache eviction

This work can be executed by 1 or more maintenance agents.
ALBA tries not to have 2 maintenance agents perform the same work.
This requires a bit of coordination. Hence 2 concepts are introduced:
- maintenance master
- position (number) of the maintenance process (e.g. 7 of 9)

Tasks 1 & 2 are only performed by the maintenance master.
Task 4 is performed by all maintenance agents. They don't run into each others way due to how they select which objects to repair.
Tasks 3, 5, 6 & 7 are divided amongst the several agents based on the namespace_id.
Task 8 is divided amongst the several agents based on the work_id.

The self healing has some default parameters but these can be overridden through the ALBA CLI.

## Retrieve the maintenance config
Run the **alba get-maintenance-config** command with as option the config of the config file of the ABM Arakoon. (See [Arakoon config files](../../Administration/Configs/arakoon.md) for more information.)

```
root@cmp02:~# alba get-maintenance-config --config=/opt/OpenvStorage/config/arakoon/<backendname>-abm/<backendname>-abm.cfg  --to-json
```
The result is a json with configuration
```
{"success":true,"result":{"enable_auto_repair":true,"auto_repair_timeout_seconds":900.0,"auto_repair_disabled_nodes":[],"enable_rebalance":true}}
root@cmp02:~#
```

The different configuration fields:
* enable_auto_repair: true or false. Indicates if a broken ASD gets automatically rebuilt on the remaining ASDs. Default is true.
* auto_repair_timeout_seconds: time it takes before the maintenance process will start to rebuild the broken ASD. Default is 900 seconds
* auto_repair_disabled_nodes: list of nodes which should not automatically be repaired
* enable_rebalance: true or false. Indicates if existing fragments should be automatically moved to new ASDs when the new ASDs are added. Default is true.

## Update the ALBA maintenance config
To change the configuration of the ALBA maintenance, run the **alba update-maintenance-config** command with as option the config of the config file of the ABM Arakoon and the parameters you cant to change.

For example to update the repair timeout
```
root@cmp02:~# alba get-maintenance-config --config=/opt/OpenvStorage/config/arakoon/<backendname>-abm/<backendname>-abm.cfg  --auto-repair-timeout-seconds=100

Maintenance config now is { Maintenance_config.enable_auto_repair = true;
  auto_repair_timeout_seconds = 100.; auto_repair_disabled_nodes = [];
  enable_rebalance = true }
```

The different option are:
* `--auto-repair-add-disabled-node=<node_id of the ASD node>` : add a node to the list of nodes which should not be automatically rebuild.
* `--auto-repair-remove-disabled-node=<node_id of the ASD node>` : remove a node to the list of nodes which should not be automatically rebuild.
* `--auto-repair-timeout-seconds=<int>` : interval before the automated repair starts rebuilding data.
* `--disable-auto-repair` :  diskable the self healing functionality.
* `--disable-rebalance` : disable the rebalancer.
* `--enable-auto-repair` : enable the self healing functionality.
* `--enable-rebalance` : enable the rebalancer.

The ASD node ID can be retrieved from the Backend Detail page in the GUI or by executing in the shell of the ASD node following command:
```
cat /opt/alba-asdmanager/config/config.json | grep node_id
```
## Rebalancing
Alba slightly prefers emptier OSDs for fragments on new writes, and thus tries to achieve a good balance. This is not enough. For example, after replacing a defect drive in a well filled Alba, the new drive will be empty and way below the average fill rate. So the maintenance agents actively rebalance the drives.

Currently the strategy is this. The drives are categorized to be in one of three buckets: **low**, **ok**, **high**.

Then, a batch of random manifests are fetched. For each of the manifests, the rebalancer tries to find a move of the fragments on an OSD in **high** towards an OSD from **low**. In absence of such a move, a less ambitious move (**high** to **ok** or **ok** to **low**) is proposed for that manifest. The batch is sorted according to the possible moves, and only a small fraction of moves (the very best ones) is actually executed. Then the process is repeated until the fill rates are acceptable.

The reasoning behind it is that it is rather cheap to fetch manifests and do some calculations, but it is expensive to move fragments and update the manifests accordingly. We really want a move to count. Also we want to avoid a combination of moving from **high** to **ok** and next from **ok** to **low** where a single move from **high** to **low** would have been possible.

To stop the rebalancing process execute:
```
root@cmp02:~# alba get-maintenance-config --config=/opt/OpenvStorage/config/arakoon/<backendname>-abm/<backendname>-abm.cfg  --disable-rebalance
```



