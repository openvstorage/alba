ACLOCAL_AMFLAGS = -I m4

lib_LTLIBRARIES = libalbaproxy.la

libalbaproxy_la_CXXFLAGS = -std=c++11
libalbaproxy_la_CPPFLAGS = -I$(abs_top_srcdir)/../include
#libalbaproxy_la_LDFLAGS = -static

libalbaproxy_la_SOURCES = \
        ../src/lib/alba_common.cc \
	../src/lib/alba_logger.cc \
	../src/lib/checksum.cc \
	../src/lib/generic_proxy_client.cc \
	../src/lib/io.cc \
	../src/lib/llio.cc \
	../src/lib/statistics.cc \
	../src/lib/manifest.cc \
	../src/lib/manifest_cache.cc \
	../src/lib/osd_access.cc \
	../src/lib/osd_info.cc \
	../src/lib/proxy_sequences.cc \
	../src/lib/proxy_client.cc \
	../src/lib/proxy_protocol.cc \
	../src/lib/rdma_proxy_client.cc \
	../src/lib/rora_proxy_client.cc \
	../src/lib/stuff.cc \
	../src/lib/tcp_proxy_client.cc

albadir = $(includedir)/alba

alba_HEADERS = \
	../include/alba_common.h \
	../include/alba_logger.h \
	../include/boolean_enum.h \
	../include/checksum.h \
	../include/generic_proxy_client.h \
	../include/io.h \
	../include/llio.h \
	../include/statistics.h \
	../include/manifest.h \
	../include/osd_info.h \
	../include/proxy_sequences.h \
	../include/proxy_client.h \
	../include/proxy_protocol.h \
	../include/rdma_proxy_client.h \
	../include/stuff.h \
	../include/tcp_proxy_client.h
