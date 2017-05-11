(*
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
*)

open! Prelude

external _partial_read_job :
  string (* fn *)
  -> int (* n elements *)
  -> (int * int) list (* [(offset, len)] *)
  -> int (* socket *)
  -> bool (* use_fadvise *)
  -> unit Lwt_unix.job = "alba_partial_read_job"


let partial_read fn n offset_sizes socket use_fadvise =
  Lwt_unix.run_job (_partial_read_job fn n offset_sizes socket use_fadvise)
