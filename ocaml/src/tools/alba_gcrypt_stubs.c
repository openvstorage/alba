/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
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
