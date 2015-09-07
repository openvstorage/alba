/*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*/

#include <errno.h>
#include <gcrypt.h>
#include <pthread.h>

GCRY_THREAD_OPTION_PTHREAD_IMPL;

int gcrypt_set_threads() {
  return gcry_control (GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
}
