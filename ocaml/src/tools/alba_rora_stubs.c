/*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*/

#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/threads.h>
#include <stdio.h>
#include <inttypes.h>
#include <rocksdb/c.h>
#include <string.h>
#include <assert.h>
#include <gobjfs_server.h>
#include <stdbool.h>

static void* _DB_POINTER = NULL;

CAMLprim value alba_register_db( value v )
{
    CAMLparam1(v);
    uint64_t i;
    i = Long_val(v);
    _DB_POINTER= (void*) i;
    return Val_unit;
}

/**
    new_name needs to be a '\0' terminated C-string
    that fits in the buffer allocated by the caller.
    (a ~4KB buffer is more than enough)
*/

int alba_rocks_db_transformer(const char* old,
                         /*const*/ int old_len,
                         char* new_name){
    printf("rocks_db_transformer (DB pointer: %p)\n", _DB_POINTER);
    fflush(stdout);
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    size_t len;
    char *err = NULL;
    char *returned_value =
        rocksdb_get(_DB_POINTER, readoptions, old, old_len, &len, &err);
    if(err){
        printf("err:%s\n",err);
        fflush(stdout);
        free(err);
        return 1;
    }
    printf("new_name:%s", new_name);
    fflush(stdout);
    assert(len < 4096);
    strcpy(returned_value,new_name);
    free(returned_value);
    return 0;
}

int64_t alba_alternative_start(const char* transport,
                               const char* host,
                               const int port,
                               const int cores,
                               const int queue_depth
                               ){
    void* x = gobjfs_xio_server_start(
                                      transport,
                                      host,
                                      port,
                                      cores,
                                      queue_depth,
                                      (void*) &alba_rocks_db_transformer,
                                      false
                                      );
    return (int64_t)x;
}


CAMLprim value alba_start_rora_server(value v_transport,
                                      value v_host,
                                      value v_port,
                                      value v_number_cores,
                                      value v_queue_depth
    ){
    CAMLparam5(v_transport, v_host, v_port, v_number_cores, v_queue_depth);
    CAMLlocal1(ml_xi);
    /*const*/ void* file_translator_func = &alba_rocks_db_transformer;
    void* x = gobjfs_xio_server_start(
        String_val(v_transport),
        String_val(v_host),
        Int_val(v_port),
        Int_val(v_number_cores),
        Int_val(v_queue_depth),
        file_translator_func,
        false
        );
    if (x == 0){
       caml_failwith("could not start rora server");
    }
    int64_t xi = (int64_t) x;
    ml_xi = caml_copy_int64(xi);
    CAMLreturn(ml_xi);

}

CAMLprim value alba_stop_rora_server(value v_handle){
    CAMLparam1(v_handle);
    const int64_t handle = Int64_val(v_handle);
    void* p = (void*) handle;
    int rc = gobjfs_xio_server_stop(p);
    return Val_int(rc);
}
