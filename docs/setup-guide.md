Very short alba setup guide
----------------

albamgr is the top level in the alba architecture.
it's an arakoon cluster configured with the plugin 'albamgr_plugin'
(add to plugins property in arakoon cfg file)

alba requires 1 or more nsm hosts
an nsm host is an arakoon configured with the plugin 'nsm_host_plugin'

nsm hosts should be registered with the albamgr
this can be done with `alba.native add-nsm-host ...`
have a look at fabfile.py for an example or use --help
using `alba.native list-nsm-hosts` you can see the registered nsm hosts.

the data is stored on asds (alba storage devices).
alba storage devices can be started with `alba.native asd-start ...`
(again, look fabfile.py or use --help)
alba storage devices have to be registered with the albamgr
this is done with `alba.native add-osd ...`
to see the effect of your actions use `alba.native list-osds`


If you made it this far you can start playing around.
First, create a namespace with `alba.native create-namespace demo`.
Next you need to link some (or all) of the asds with this namespace,
this can be done with `alba.native add-ns-osd ...`.
Other (sub) commands you can now play with are list-object, upload-object, ...
