set -e
set -v

. ./_virt/bin/activate
rm -f ./gtestresults.xml

export LD_LIBRARY_PATH=${PWD}/cpp/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib

fab dev.run_tests_cpp:xml=True || true
