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

#include <errno.h>
#include <gcrypt.h>
#include <pthread.h>

/* 
 * NOTE form /usr/include/gcrypt.h:
 *   "Since Libgcrypt 1.6 the thread callbacks are not anymore
 *    used. However we keep it to allow for some source code
 *    compatibility if used in the standard way."
 *
 * we keep this in here to support the older grcrypt versions on centos-7
 */

GCRY_THREAD_OPTION_PTHREAD_IMPL;

int gcrypt_set_threads() {
  return gcry_control (GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
}
