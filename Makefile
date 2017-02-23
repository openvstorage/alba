PREFIX ?=/usr

START = $(DESTDIR)$(PREFIX)
LIB = $(START)/lib/alba

all: build

clean:
	cd ocaml && ocamlbuild -clean
	cd setup && ocamlbuild -clean
	rm -f ./ocaml/alba.native \
              ./setup/setup.native \
              ./ocaml/disk_failure_tests.native


build: build-alba build-cmxs build-nsm-plugin build-mgr-plugin setup

build-alba:
	cd ocaml && ocamlbuild -j 0 -use-ocamlfind alba.native disk_failure_tests.native

build-cmxs: build-alba
	cd ocaml && ocamlbuild -use-ocamlfind \
	albamgr_protocol.cmx \
	albamgr_plugin.cmx \
	nsm_host_plugin.cmx

build-nsm-plugin: build-cmxs
	cd ocaml && ocamlfind ocamlopt \
	_build/alba_version.cmx \
        _build/src/range_query_args.cmx \
	_build/src/tools/lwt_extra2.cmx \
	_build/src/tools/prelude.cmx \
	_build/src/tools/deser.cmx \
	_build/src/tools/cache.cmx \
	_build/src/tools/weak_pool.cmx \
	_build/src/tools/buffer_pool.cmx \
	_build/src/tools/tls.cmx \
	_build/src/tools/alba_llio.cmx \
	_build/src/model/fragment_size_helper.cmx \
	_build/src/model/consistency.cmx \
	_build/src/model/policy.cmx \
	_build/src/model/encryption.cmx \
	_build/src/model/checksum.cmx \
	_build/src/model/alba_compression.cmx \
	_build/src/model/preset.cmx \
	_build/src/model/fragment.cmx \
	_build/src/arith64.cmx \
	_build/src/key_value_store.cmx \
	_build/src/mem_key_value_store.cmx \
	_build/src/osd_keys.cmx \
	_build/src/alba_arakoon.cmx \
	_build/src/model/layout.cmx \
        _build/src/nsm_model.cmx \
	_build/src/tools/stat.cmx \
	_build/src/alba_statistics.cmx \
	_build/src/tools/statistics_collection.cmx \
	_build/src/nsm_protocol.cmx \
	_build/src/nsm_host_protocol.cmx \
	_build/src/plugin_extra.cmx \
	_build/src/nsm_host_plugin.cmx \
	-linkpkg -package ppx_deriving_yojson \
        -linkpkg -package uuidm \
        -linkpkg -package result \
	-shared -o nsm_host_plugin.cmxs

build-mgr-plugin: build-alba
	cd ocaml && ocamlfind ocamlopt \
	_build/alba_version.cmx \
	_build/src/tools/lwt_extra2.cmx \
	_build/src/tools/prelude.cmx \
	_build/src/range_query_args.cmx \
	_build/src/tools/deser.cmx \
	_build/src/tools/cache.cmx \
	_build/src/tools/weak_pool.cmx \
	_build/src/tools/buffer_pool.cmx \
	_build/src/tools/tls.cmx \
	_build/src/tools/alba_llio.cmx \
	_build/src/model/fragment_size_helper.cmx \
	_build/src/model/consistency.cmx \
	_build/src/model/policy.cmx \
	_build/src/model/encryption.cmx \
	_build/src/model/checksum.cmx \
	_build/src/model/alba_compression.cmx \
	_build/src/model/preset.cmx \
	_build/src/model/fragment.cmx \
	_build/src/alba_arakoon.cmx \
	_build/src/arith64.cmx \
	_build/src/key_value_store.cmx \
	_build/src/mem_key_value_store.cmx \
	_build/src/plugin_extra.cmx \
	_build/src/log_plugin.cmx \
	_build/src/osd_keys.cmx \
	_build/src/model/layout.cmx \
	_build/src/nsm_model.cmx \
	_build/src/tools/stat.cmx \
	_build/src/alba_statistics.cmx \
	_build/src/tools/statistics_collection.cmx \
	_build/src/nsm_protocol.cmx \
	_build/src/nsm_host_protocol.cmx \
	_build/src/maintenance_config.cmx \
	_build/src/albamgr_protocol.cmx \
	_build/src/albamgr_plugin.cmx \
	-linkpkg -package ppx_deriving_yojson \
	-linkpkg -package tiny_json \
	-linkpkg -package uuidm \
        -linkpkg -package result \
	-shared -o albamgr_plugin.cmxs

setup:
	cd ./setup && ocamlbuild -use-ocamlfind setup.native

install: build-alba
	mkdir -p $(START)/bin/
	cp ./ocaml/alba.native $(START)/bin/alba
	mkdir -p $(START)/lib/alba/
	echo $(START)

	echo $(LIB)
	for i in Jerasure rocksdb isal gf_complete; \
        do ldd ./ocaml/alba.native \
           | grep $$i \
           | grep "=> /" \
           | awk '{print $$3}' \
           | xargs -I{} cp -v "{}" $(LIB) ;\
        done

	cp ./ocaml/albamgr_plugin.cmxs         $(LIB)/albamgr_plugin.cmxs
	cp ./ocaml/nsm_host_plugin.cmxs        $(LIB)/nsm_host_plugin.cmxs
	mkdir -p $(DESTDIR)/etc/ld.so.conf.d
	echo '/usr/lib/alba/' > $(DESTDIR)/etc/ld.so.conf.d/alba.conf

uninstall:
	rm $(START)/local/lib/alba/*
	rm $(START)/bin/alba

doc :
	emacs -u $(USER) --script ./docs/export.el

.PHONY: install build doc setup
