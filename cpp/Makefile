CXX ?= g++
ALBA_HOME ?=../

# g++ 5 and clang++-3.7 have binary compatibility problems
# & dynamic linking with boost libraries causes arbitrary segv's for g++
# So this is currently the GCD : statically link the boost stuff

# Also, when you've done something wrong, level 3 is more likely to SEGV.
# so it's probably a better test
OPTIMIZATION = 3

WARNINGS = \
	-Wall -Wextra -Wno-unknown-pragmas -Wctor-dtor-privacy -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor
FLAGS = -std=c++14 -ggdb3 -gdwarf-3 -O$(OPTIMIZATION) -fPIC

CMD = $(CXX) $(WARNINGS) $(FLAGS) -I./include

LIBDIRS = -L/usr/lib/x86_64-linux-gnu -L/usr/local/lib \
          -L/usr/lib

LIBS_lib = -lboost_system -lboost_thread -lboost_log -lpthread -lboost_program_options \
           -lsnappy -lrdmacm

LIBS_exec = -L/usr/local/lib \
	-Wl,-Bstatic \
	  -lboost_log -lboost_system -lboost_thread -lboost_program_options \
	-Wl,-Bdynamic \
        -L./lib -lalba -lrdmacm -lpthread \
        -lsnappy -lgcrypt

_OBJECTS = alba_common.o stuff.o manifest.o alba_logger.o \
           proxy_protocol.o llio.o checksum.o \
           proxy_sequences.o \
           io.o proxy_client.o generic_proxy_client.o rora_proxy_client.o \
           transport_helper.o \
	   osd_info.o manifest_cache.o osd_access.o statistics.o \
	   asd_client.o asd_protocol.o rdma_transport.o tcp_transport.o transport.o \
	   asd_access.o encryption.o

OBJECTS = $(patsubst %,src/lib/%,$(_OBJECTS))


all: compile_lib compile_tests

	mkdir -p ./lib
	$(CXX) -shared -Wl,-soname,libalba.so.1 \
	    $(LIBDIRS) $(LIBS_lib) \
	    -o lib/libalba.so.1.0.1 $(OBJECTS)

	cd lib && ln -f -s libalba.so.1.0.1 libalba.so.1
	cd lib && ln -f -s libalba.so.1 libalba.so

	mkdir -p ./bin

	$(CXX) \
            src/tests/llio_test.o \
	    src/tests/proxy_client_test.o \
	    src/tests/asd_client_test.o \
	    src/tests/main.o \
	    $(LIBDIRS) \
            $(LIBS_exec) -lgtest -lrdmacm \
	    -o bin/unit_tests.out

	$(CXX) \
	    src/examples/test_client.o \
	    $(LIBDIRS) $(LIBS_exec) -o bin/test_client.out

%.o: %.cc
	$(CMD) -c $< -o $@

compile_lib: $(OBJECTS)

compile_tests:
	$(CMD) -I/usr/include/gtest \
	-c src/tests/llio_test.cc -o src/tests/llio_test.o

	$(CMD) -I/usr/include/gtest -I./src/lib/ \
	-c src/tests/proxy_client_test.cc -o src/tests/proxy_client_test.o

	$(CMD) -I/usr/include/gtest \
	-c src/tests/asd_client_test.cc -o src/tests/asd_client_test.o

	$(CMD) -I/usr/include/gtest \
	-c ./src/tests/main.cc -o src/tests/main.o

	$(CMD) -c src/examples/test_client.cc -o src/examples/test_client.o



clean:
	find -name "*.o" | xargs rm -f
	rm -rf ./lib/*
	rm -rf ./bin/*

format:
	find -name "*.cc" | xargs clang-format-3.8 -i
	find -name "*.h"  | xargs clang-format-3.8 -i
