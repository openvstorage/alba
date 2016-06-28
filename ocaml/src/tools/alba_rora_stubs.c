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
#include <sys/stat.h>

static void* _DB_POINTER = NULL;
static char* _ROOT_PATH = NULL;
static int   _ROOT_PATH_LENGTH = -1;

CAMLprim value alba_register_rocksdb(value v_rocks){
    CAMLparam1(v_rocks);
    const uint64_t raw = Nativeint_val(v_rocks);
    _DB_POINTER = (void*) raw;
    return Val_unit;
}


char hexlify(uint8_t c) {
    char r;
    if (c < 10) {
        r = c + 48;
    } else {
        r = c + 87;
    }
    return r;
}

void convert_hex(uint8_t c, char* tgt) {
    char h0 = hexlify(c >> 4);
    char h1 = hexlify(c & 0x0f);
    tgt[0] = h0;
    tgt[1] = h1;
}


int _get_fnr_flen(char* returned_value, uint64_t *fnr, uint32_t *flen){
    int pos = 0;

    uint8 checksum_type = returned_value[pos++];
    switch(checksum_type){
    case 1: { /* NoChecksum  */      }; break;
    case 2: { /* Sha1  */  pos += 24;}; break;
    case 3: { /* Crc32c */ pos +=  4;}; break;
    default:{return 1;}
    }

    uint8 blob_type = returned_value[pos++];
    switch(blob_type){
    case 1: { /* DirectValue */ return 2; }; break;// We don't support this.
    case 2: { /* onFs */   }; break;
    default:{ return 3;};
    }

    char* fnr_addr       = &returned_value[pos];
    uint64_t* fnr_addr64 = (uint64_t*) fnr_addr;
    uint64_t _fnr = *fnr_addr64;
    *fnr = _fnr;
    pos += 8;
    char* flen_addr = &returned_value[pos];
    uint32_t* flen_addr32 = (uint32_t*) flen_addr;
    uint32_t _flen = *flen_addr32;
    *flen = _flen;
    return 0;
}

int file_exists (char * fileName)
{
    struct stat buf;
    int i = stat ( fileName, &buf );
    /* File found */
    if ( i == 0 )
    {
        return 1;
    }
    return 0;

}


/**
    new_name needs to be a '\0' terminated C-string
    that fits in the buffer allocated by the caller.
    (a ~4KB buffer is more than enough)
*/

int alba_rocks_db_transformer(const char* old,
                         /*const*/ int old_len,
                         char* new_name){
    printf("rocks_db_transformer (DB pointer: %p) old_len:%i\n", _DB_POINTER, old_len);
    fflush(stdout);
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    if (old_len <= 0){
        return -1;
    }
    // add the prefix for public keys.
    int public_key_len = old_len + 1;
    char* public_key = (char*) malloc(public_key_len);

    public_key[0] = '\x00';
    memcpy(&public_key[1], old, old_len);

    size_t len;
    char *err = NULL;
    char *returned_value =
        rocksdb_get(_DB_POINTER, readoptions, public_key, public_key_len, &len, &err);

    if(err){
        printf("err:%s\n",err);
        fflush(stdout);
        free(err);
        return -1;
    }
    if(returned_value == NULL){
        return -1;
    }

    uint32_t fragment_length;
    uint64_t fnr;
    _get_fnr_flen(returned_value, &fnr, &fragment_length);
    assert(len < 4096);
    int result = -1;
    if (returned_value != NULL){
        memcpy(new_name,_ROOT_PATH,_ROOT_PATH_LENGTH);
        size_t pos = 0;
        size_t new_name_pos = _ROOT_PATH_LENGTH;
        new_name[new_name_pos++] = '/';

        uint8_t cs[] = {0,0,0,0, 0,0,0,0};
        int i;
        for(i= 7;i>=0;i--){
            cs[i] = fnr & 0xff;
            fnr = fnr >> 8;
        }
        while(pos < 7){
            uint8_t c = cs[pos];
            convert_hex(c, &new_name[new_name_pos]);
            pos ++;
            new_name_pos += 2;
            new_name[new_name_pos++] = '/';
        }
        sprintf(&new_name[new_name_pos], "%016x", cs[7]);
        /*
        if(file_exists(new_name)==1){
            printf("this file EXISTS\n");fflush(stdout);
        }
        */
        free(returned_value);
        result = 0;
    }
    free(public_key);
    return result;
}

int64_t alba_alternative_start(const char* transport,
                               const char* host,
                               const int port,
                               const int cores,
                               const int queue_depth,
                               const char* root_path,
                               const int root_path_len
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
    _ROOT_PATH=(char*)malloc(root_path_len);
    _ROOT_PATH_LENGTH = root_path_len;
    memcpy(_ROOT_PATH,root_path,_ROOT_PATH_LENGTH);

    return (int64_t)x;
}


CAMLprim value alba_stop_rora_server(value v_handle){
    CAMLparam1(v_handle);
    const int64_t handle = Int64_val(v_handle);
    void* p = (void*) handle;
    int rc = gobjfs_xio_server_stop(p);
    free(_ROOT_PATH);
    return Val_int(rc);
}
