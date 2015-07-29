ACLOCAL_AMFLAGS = -I m4

lib_LTLIBRARIES = libalbaproxy.la

libalbaproxy_la_CXXFLAGS = -std=c++11
libalbaproxy_la_CPPFLAGS = -I../include
#libalbaproxy_la_LDFLAGS = -static

libalbaproxy_la_SOURCES = \
	../src/lib/alba_logger.cc \
	../src/lib/io.cc \
	../src/lib/llio.cc \
	../src/lib/checksum.cc \
	../src/lib/proxy_client.cc \
	../src/lib/proxy_protocol.cc \
	../src/lib/stuff.cc


albadir = $(includedir)/alba

alba_HEADERS = \
	../include/alba_logger.h \
	../include/boolean_enum.h \
	../include/io.h \
	../include/llio.h \
	../include/checksum.h \
	../include/proxy_client.h \
	../include/proxy_protocol.h \
	../include/stuff.h
