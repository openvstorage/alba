# C++ Client

ALBA has a C++ client. Usage of the client should be straightforward.

Sample usage:
```
# Create the client
auto client = make_proxy_client(host, port, timeout, transport);

# Read an object
client->read_object_fs(ns, name, file, _consistent_read, _should_cache);

# Write an object
client->write_object_fs(ns, name, file,
                            alba::proxy_client::allow_overwrite::T, nullptr);
```

A sample program can be found [here](https://github.com/openvstorage/alba/blob/master/cpp/src/examples/test_client.cc).

The ALBA proxy client interface can be found [here](https://github.com/openvstorage/alba/blob/master/cpp/include/proxy_client.h).