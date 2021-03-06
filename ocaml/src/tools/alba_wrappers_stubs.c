/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#include <sys/resource.h>

#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/threads.h>

#include <string.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>

/* maybe when we need more values from getrusage,
   we can expose the whole struct. in that case, ctypes
   would make sense.
*/
CAMLprim value alba_get_maxrss(value unit) {
    CAMLparam1(unit);
    CAMLlocal1(vres);

    //caml_release_runtime_system();

    int res = 0, who = RUSAGE_SELF;
    struct rusage usage;

    res = getrusage(who, &usage);

    if (res != 0){
        caml_failwith ("get_rusage");
    }

    //caml_acquire_runtime_system ();

    vres = Val_int(usage.ru_maxrss);
    CAMLreturn(vres);
}

// http://stackoverflow.com/questions/6583158/finding-open-file-descriptors-for-a-process-linux-c-code
CAMLprim value alba_get_num_fds(value unit){
    CAMLparam1(unit);
    CAMLlocal1(vres);

    //caml_release_runtime_system();
    int fd_count;
    char buf[64];
    struct dirent *dp;

    snprintf(buf, 64, "/proc/%i/fd/", getpid());

    fd_count = 0;
    DIR *dir = opendir(buf);
    while ((dp = readdir(dir)) != NULL) {
        fd_count++;
    }
    closedir(dir);

    //caml_acquire_runtime_system ();
    vres = Val_int(fd_count);
    CAMLreturn(vres);
    //return fd_count;
}
