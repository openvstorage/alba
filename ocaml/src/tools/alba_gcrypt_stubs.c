/*
Copyright 2015 iNuron NV

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

#include <errno.h>
#include <gcrypt.h>
#include <pthread.h>

GCRY_THREAD_OPTION_PTHREAD_IMPL;

int gcrypt_set_threads() {
  return gcry_control (GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
}
