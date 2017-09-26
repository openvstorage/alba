(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

external _partial_read_job :
  string (* fn *)
  -> int (* n elements *)
  -> (int * int) list (* [(offset, len)] *)
  -> int (* socket *)
  -> bool (* use_fadvise *)
  -> float Lwt_unix.job = "alba_partial_read_job"


let partial_read fn n offset_sizes socket use_fadvise =
  Lwt_unix.run_job (_partial_read_job fn n offset_sizes socket use_fadvise)
