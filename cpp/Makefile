CXX ?= g++
ALBA_HOME ?=../
GOBJFS_HOME ?=$(ALBA_HOME)/../gobjfs

# g++ 5 and clang++-3.7 have binary compatibility problems
# & dynamic linking with boost libraries causes arbitrary segv's for g++
# So this is currently the GCD : statically link the boost stuff

OPTIMIZATION = 2
WARNINGS = \
	-Wall -Wextra -Wno-unknown-pragmas -Wctor-dtor-privacy -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor
FLAGS = -std=c++11 -ggdb3 -gdwarf-3 -O$(OPTIMIZATION) -fPIC

CMD = $(CXX) $(WARNINGS) $(FLAGS) -I./include -I$(GOBJFS_HOME)/include

LIBDIRS = -L/usr/lib/x86_64-linux-gnu -L/usr/local/lib \
          -L/usr/lib \
          -L$(GOBJFS_HOME)/lib

LIBS_lib = -lboost_system -lboost_thread -lboost_log -lpthread -lboost_program_options \
           -lsnappy \
	   -lgobjfs_client -lxio

LIBS_exec = -L/usr/local/lib \
	-Wl,-Bstatic \
	  -lboost_log -lboost_system -lboost_thread -lboost_program_options \
	-Wl,-Bdynamic \
	-L./lib -lalba -lrdmacm -lpthread \
	-lsnappy -lgobjfs_client -lxio

_OBJECTS = alba_common.o stuff.o manifest.o alba_logger.o \
           proxy_protocol.o llio.o checksum.o \
           proxy_sequences.o \
           io.o proxy_client.o generic_proxy_client.o \
	   tcp_proxy_client.o rdma_proxy_client.o rora_proxy_client.o \
	   osd_info.o manifest_cache.o osd_access.o statistics.o

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

	$(CMD) -I/usr/include/gtest \
	-c src/tests/proxy_client_test.cc -o src/tests/proxy_client_test.o

	$(CMD) -I/usr/include/gtest \
	-c ./src/tests/main.cc -o src/tests/main.o

	$(CMD) -c src/examples/test_client.cc -o src/examples/test_client.o



clean:
	find -name "*.o" | xargs rm -f
	rm -rf ./lib/*
	rm -rf ./bin/*

format:
	find -name "*.cc" | xargs clang-format -i
	find -name "*.h"  | xargs clang-format -i
