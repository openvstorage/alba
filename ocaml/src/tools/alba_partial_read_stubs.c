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

#include <string.h>
#include <errno.h>
#include "lwt_unix.h"
#include <sys/stat.h>

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>

#include <stdio.h>
#include <fcntl.h>
#include <sys/sendfile.h>
#include <assert.h>

struct job_partial_read {
  /*  used by lwt.
      MUST be the first field of the structure. */
  struct lwt_unix_job job;

  int result;
  int errno_copy;
  int n_slices;
  char* path;
  int socket;
  int64_t *offsets;
  int* lens;
  int use_fadvise;
  /* Buffer for storing the path. MUST be last field */
  char data[];
};

static void worker_partial_read(struct job_partial_read* job)
{
    /* Perform the blocking call. */
    int in_fd = open(job -> path, O_RDONLY);
    if(-1 == in_fd){
        job -> result = -1;
        job -> errno_copy = errno;
        return;
    }
    int pos;
    int n = job -> n_slices;
    if(job -> use_fadvise){
        int r;
        r = posix_fadvise(in_fd,
                          0/*from begin*/,
                          0/* end of file */,
                          POSIX_FADV_RANDOM);
        if(r){
            job -> result = r;
            job -> errno_copy = r; // syscall doesn't use errno.
            return;
        }
        pos = 0;
        while (pos < n){
            r = posix_fadvise(in_fd,
                              job -> offsets[pos],
                              job -> lens[pos],
                              POSIX_FADV_WILLNEED);
            if (r){
                job -> result = r;
                job -> errno_copy = r;
                return;
            }
            pos++;
        }

    }
    pos = 0;
    while (pos < n ){
        int64_t offset = job -> offsets[pos];
        int todo       = job -> lens[pos];

        do {
            int sent = sendfile(job -> socket, in_fd, &offset, todo);
            if(sent == -1){
                job -> result = -1;
                job -> errno_copy = errno;
                close(in_fd);
                return;
            }
            todo = todo - sent;
        } while(todo > 0);
        pos++;
    }
    if(job -> use_fadvise){
        pos = 0;
        int r;
        while (pos < n){
            r = posix_fadvise(in_fd,
                              job -> offsets[pos],
                              job -> lens[pos],
                              POSIX_FADV_DONTNEED);
            if (r) {
                job -> result = r;
                job -> errno_copy = r;
                return;
            }
            pos++;
        }
    }
    close(in_fd);// TODO: do we care if this fails? (what can we do?)
    job -> result = 0;
}

/* The function building the caml result. */
static value result_partial_read(struct job_partial_read* job){
  free(job -> offsets);
  free(job -> lens);
  /* Check for errors. */
  if (job->result < 0) {
    /* Save the value of errno so we can use it
       once the job has been freed. */
    int error = job->errno_copy;
    /* Copy the contents of job->path into a caml string. */
    value string_argument = caml_copy_string(job->path);
    /* Free the job structure. */
    lwt_unix_free_job(&job->job);
    /* Raise the error. */
    unix_error(error, "partial_read", string_argument);
  }
  /* Free the job structure. */
  lwt_unix_free_job(&job->job);
  /* Return the result. */
  return Val_unit;
}


CAMLprim value alba_partial_read_job(value path,
                                     value n_slices,
                                     value offset_lens, /* [(offset,len)] */
                                     value socket,
                                     value use_fadvise
    ){

  LWT_UNIX_INIT_JOB_STRING(job, partial_read,0, path);

  job->job.worker = (lwt_unix_job_worker)worker_partial_read;
  job->job.result = (lwt_unix_job_result)result_partial_read;

  job -> socket = Int_val(socket);
  int n_slices_ = Int_val(n_slices);
  job -> n_slices = n_slices_;
  job -> offsets = malloc(sizeof(int64_t) * n_slices_);
  job -> lens    = malloc(sizeof(int) * n_slices_);
  job -> use_fadvise = Bool_val(use_fadvise);
  int pos = 0;

  while (offset_lens != Val_emptylist){
      value v_head = Field(offset_lens, 0);
      value v_off  = Field(v_head, 0);
      value v_size = Field(v_head, 1);

      job -> offsets[pos] = Int_val(v_off);
      job -> lens[pos]    = Int_val(v_size);
      pos ++;
      offset_lens= Field(offset_lens, 1);
      }


  return lwt_unix_alloc_job(&job->job);
}
