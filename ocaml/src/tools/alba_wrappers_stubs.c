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

#include <sys/resource.h>

#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/threads.h>



/* maybe when we need more values from getrusage,
   we can expose the whole struct. in that case, ctypes
   would make sense.
*/
CAMLprim value alba_get_maxrss(value unit) {
    CAMLparam1 (unit);

    caml_release_runtime_system();

    int res = 0, who = RUSAGE_SELF;
    struct rusage usage;

    res = getrusage(who, &usage);

    if (res != 0){
        caml_failwith ("get_rusage");
    }

    caml_acquire_runtime_system ();

    CAMLreturn(Val_int(usage.ru_maxrss));
}
