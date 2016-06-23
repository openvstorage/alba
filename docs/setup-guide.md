Quick install guide
----------------

albamgr is the top level in the alba architecture.
it's an arakoon cluster configured with the plugin 'albamgr_plugin'
(add `plugins = albamgr_plugin ` in the `[global]` section of the arakoon cfg file).
These alba plugins are part of the alba package and need to be linked or copied
to the arakoon_home_dir prior to adding it to the arakoon config file.

Alba requires 1 or more nsm hosts,
an nsm host is an arakoon configured with the plugin 'nsm_host_plugin'

nsm hosts should be registered with the albamgr
this can be done with `alba add-nsm-host ...`
have a look at fabfile.py for an example or use --help
using `alba list-nsm-hosts` you can see the registered nsm hosts.

The data is stored on asds (alba storage devices).
alba storage devices can be started with `alba asd-start ...`
(again, look fabfile.py or use --help)
alba storage devices have to be registered with the albamgr
this is done with `alba add-osd ...`
to see the effect of your actions use `alba list-osds`.
Done this these devices can be claimed into a backend using `alba claim-osd ...`

If you made it this far you can start playing around.
First, create a namespace with `alba create-namespace demo`.
Next you need to link some (or all) of the asds with this namespace,
this can be done with `alba add-ns-osd ...`.
Other (sub) commands you can now play with are list-object, upload-object, ...


Default presets are included but you might want to create your own or customize
them, one can do this by using `alba create-preset ...` or `alba update-preset ...`

Example configs for asd, proxy, presets, etc. can be found in [alba/cfg](https://github.com/openvstorage/alba/tree/master/cfg)
