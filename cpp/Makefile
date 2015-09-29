CXX = clang++-3.5
WARNINGS = \
	-Wall -Wextra -Wno-unknown-pragmas -Wctor-dtor-privacy -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor
FLAGS = -std=c++11 -ggdb3 -gdwarf-3 -O3 -fPIC -DBOOST_LOG_DYN_LINK

CMD = $(CXX) $(WARNINGS) $(FLAGS) -I./include

LIBS = -lboost_system -lboost_thread -lboost_log -lpthread -lboost_program_options
all:
	$(CMD) -c src/lib/stuff.cc -o src/lib/stuff.o
	$(CMD) -c src/lib/alba_logger.cc -o src/lib/alba_logger.o
	$(CMD) -c src/lib/proxy_protocol.cc -o src/lib/proxy_protocol.o
	$(CMD) -c src/lib/llio.cc -o src/lib/llio.o
	$(CMD) -c src/lib/checksum.cc -o src/lib/checksum.o
	$(CMD) -c src/lib/io.cc -o src/lib/io.o
	$(CMD) -c src/lib/proxy_client.cc -o src/lib/proxy_client.o

	mkdir -p ./lib
	$(CXX) -shared -Wl,-soname,libalba.so.1 \
	    -o lib/libalba.so.1.0.1 src/lib/alba_logger.o \
	    src/lib/checksum.o src/lib/io.o src/lib/llio.o src/lib/proxy_client.o \
	    src/lib/proxy_protocol.o src/lib/stuff.o \
	    -L/usr/local/lib -L/usr/lib/x86_64_linux-gnu \
	    -L/usr/lib -L/alba/lib \
	    $(LIBS)

	cd lib && ln -f -s libalba.so.1.0.1 libalba.so.1
	cd lib && ln -f -s libalba.so.1 libalba.so

	$(CXX) $(WARNINGS) $(FLAGS) -I./include -I/usr/include/gtest -I./src/lib \
	    -c src/tests/llio_test.cc -o src/tests/llio_test.o

	$(CXX) $(WARNINGS) $(FLAGS) -I./include -I/usr/include/gtest -I./src/lib \
	    -c src/tests/proxy_client_test.cc -o src/tests/proxy_client_test.o

	mkdir -p ./bin
	$(CXX) $(WARNINGS) $(FLAGS) ./src/tests/main.cc -I./include -I/usr/include/gtest \
	    -L./lib -lalba /usr/lib/libgtest.a src/tests/llio_test.o src/tests/proxy_client_test.o   -L/usr/local/lib -L/usr/lib/x86_64_linux-gnu -L/usr/lib -L/alba/lib \
	    $(LIBS) -o bin/unit_tests.out

	$(CXX) $(WARNINGS) $(FLAGS) -I./include \
	    -L./lib -lalba -L/usr/local/lib -L/usr/lib/x86_64_linux-gnu \
	    -L/usr/lib -L/alba/lib \
	    $(LIBS) \
	    src/examples/test_client.cc -o bin/test_client.out
