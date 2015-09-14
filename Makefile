PREFIX ?=/usr

START = $(DESTDIR)$(PREFIX)
LIB = $(START)/lib/alba

all: build

clean:
	rm -rf ./ocaml/_build

build: build-alba build-cmxs build-nsm-plugin build-mgr-plugin \
	build-disk-failure-tests

build-alba:
	cd ocaml && ocamlbuild -j 0 -use-ocamlfind alba.native

build-cmxs: build-alba
	cd ocaml && ocamlbuild -use-ocamlfind \
	albamgr_protocol.cmx \
	albamgr_plugin.cmx \
	nsm_host_plugin.cmx

build-nsm-plugin: build-cmxs
	cd ocaml && ocamlfind ocamlopt \
	_build/alba_version.cmx \
	_build/src/tools/prelude.cmx \
	_build/src/tools/deser.cmx \
	_build/src/consistency.cmx \
	_build/src/policy.cmx \
	_build/src/encryption.cmx \
	_build/src/tools/weak_pool.cmx \
	_build/src/tools/buffer_pool.cmx \
	_build/src/tools/lwt_extra2.cmx \
	_build/src/tools/networking2.cmx \
	_build/src/tools/checksum.cmx \
	_build/src/tools/alba_compression.cmx \
	_build/src/arith64.cmx \
	_build/src/key_value_store.cmx \
	_build/src/mem_key_value_store.cmx \
	_build/src/osd_keys.cmx \
	_build/src/nsm_model.cmx \
	_build/src/nsm_protocol.cmx \
	_build/src/nsm_host_protocol.cmx \
	_build/src/plugin_extra.cmx \
	_build/src/nsm_host_plugin.cmx \
	-linkpkg -package ppx_deriving_yojson \
	-shared -o nsm_host_plugin.cmxs

build-mgr-plugin: build-alba
	cd ocaml && ocamlfind ocamlopt \
	_build/alba_version.cmx \
	_build/src/tools/prelude.cmx \
	_build/src/tools/deser.cmx \
	_build/src/consistency.cmx \
	_build/src/policy.cmx \
	_build/src/encryption.cmx \
	_build/src/tools/weak_pool.cmx \
	_build/src/tools/buffer_pool.cmx \
	_build/src/tools/lwt_extra2.cmx \
	_build/src/tools/networking2.cmx \
	_build/src/tools/checksum.cmx \
	_build/src/tools/alba_compression.cmx \
	_build/src/arith64.cmx \
	_build/src/key_value_store.cmx \
	_build/src/mem_key_value_store.cmx \
	_build/src/plugin_extra.cmx \
	_build/src/log_plugin.cmx \
	_build/src/osd_keys.cmx \
	_build/src/nsm_model.cmx \
	_build/src/nsm_protocol.cmx \
	_build/src/nsm_host_protocol.cmx \
        _build/src/tools/stat.cmx \
        _build/src/alba_statistics.cmx \
	_build/src/albamgr_statistics.cmx \
	_build/src/albamgr_protocol.cmx \
	_build/src/albamgr_plugin.cmx \
	-linkpkg -package ppx_deriving_yojson \
	-shared -o albamgr_plugin.cmxs

build-disk-failure-tests: build-alba
	cd ocaml && ocamlbuild -use-ocamlfind disk_failure_tests.native

install: build-alba
	mkdir -p $(START)/bin/
	cp ./ocaml/alba.native $(START)/bin/alba
	mkdir -p $(START)/lib/alba/
	echo $(START)

	echo $(LIB)
	cp /usr/local/lib/libJerasure.so.2     $(LIB)/libJerasure.so.2
	cp /usr/local/lib/librocksdb.so	       $(LIB)/librocksdb.so
	cp /usr/local/lib/libgf_complete.so.1  $(LIB)/libgf_complete.so.1
	cp ./ocaml/albamgr_plugin.cmxs         $(LIB)/albamgr_plugin.cmxs
	cp ./ocaml/nsm_host_plugin.cmxs        $(LIB)/nsm_host_plugin.cmxs
	mkdir -p $(DESTDIR)/etc/ld.so.conf.d
	echo '/usr/lib/alba/' > $(DESTDIR)/etc/ld.so.conf.d/alba.conf

uninstall:
	rm $(START)/local/lib/alba/*
	rm $(START)/bin/alba

doc :
	emacs -u $(USER) --script ./doc/export.el

.PHONY: install build doc
