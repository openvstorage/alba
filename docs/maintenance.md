# Maintenance
ALBA has built-in maintenance functionality. In case an ASD is broken, fragments on the ASD will be automatically reconstructed on remaining disks in the Backend.

The maintenance work consists out of several types of maintenance work:

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
The maintenance config can be retrieved from the ALBA CLI by using the config in ETCD (`/ovs/alba/backends/<backend guid>/maintenance/config`) . (See the [Framework Alba Plugin GitBook](https://www.gitbook.com/book/openvstorage/framework-alba-plugin) for more information.)

```
root@cmp01:~# alba get-maintenance-config --config=etcd://127.0.0.1:2379/ovs/arakoon/mybackend-2-abm/config  --to-json
2016-08-01 13:27:30 637984 +0200 - cmp01 - 27641/0 - alba/cli - 0 - info - ETCD: etcdctl --peers=127.0.0.1:2379 get --quorum ovs/arakoon/mybackend-2-abm/config
{
  "success": true,
  "result": {
    "enable_auto_repair": true,
    "auto_repair_timeout_seconds": 900.0,
    "auto_repair_disabled_nodes": [],
    "enable_rebalance": true,
    "cache_eviction_prefix_preset_pairs": {},
    "redis_lru_cache_eviction": null
  }
}
```


The different configuration fields:
* enable_auto_repair: true or false. Indicates if a broken ASD gets automatically rebuilt on the remaining ASDs. Default is true.
* auto_repair_timeout_seconds: time it takes before the maintenance process will start to rebuild the broken ASD. Default is 900 seconds
* auto_repair_disabled_nodes: list of nodes which should not automatically be repaired.
* enable_rebalance: true or false. Indicates if existing fragments should be automatically moved to new ASDs when the new ASDs are added. Default is true.
* cache_eviction_prefix_preset_pairs: prefix used for cache namespaces.
* redis_lru_cache_eviction: the Redis cluster used for LRU cache eviction.

## Rebalancing
Alba slightly prefers emptier OSDs for fragments on new writes, and thus tries to achieve a good balance. This is not enough. For example, after replacing a defect drive in a well filled Alba, the new drive will be empty and way below the average fill rate. So the maintenance agents actively rebalance the drives.

Currently the strategy is this. The drives are categorized to be in one of three buckets: **low**, **ok**, **high**.

Then, a batch of random manifests are fetched. For each of the manifests, the rebalancer tries to find a move of the fragments on an OSD in **high** towards an OSD from **low**. In absence of such a move, a less ambitious move (**high** to **ok** or **ok** to **low**) is proposed for that manifest. The batch is sorted according to the possible moves, and only a small fraction of moves (the very best ones) is actually executed. Then the process is repeated until the fill rates are acceptable.

The reasoning behind it is that it is rather cheap to fetch manifests and do some calculations, but it is expensive to move fragments and update the manifests accordingly. We really want a move to count. Also we want to avoid a combination of moving from **high** to **ok** and next from **ok** to **low** where a single move from **high** to **low** would have been possible.

## The speed of rebalancing
To speed up or slow down the speed with which the rebalancing happens, update the number of maintenance agents in ETCD. The default amount is 4.

* Download the config file.
```
etcdctl get /ovs/alba/backends/<backend guid>/maintenance/nr_of_agents > /tmp/nr_of_agents.txt
```
* Update the value in the config file.
* Upload the updated config file.
```
etcdctl set  /tmp/nr_of_agents.txt > /ovs/alba/backends/<backend guid>/maintenance/nr_of_agents
```

