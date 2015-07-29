/*
Copyright 2015 Open vStorage NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
