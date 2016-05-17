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

open Prelude

let user_function (user_db : Registry.user_db) (value_o : string option) =
  match value_o with
  | None -> None
  | Some payload ->
    let buf = Llio.make_buffer payload 0 in
    let next_id_key = Llio.string_from buf in
    let log_prefix = Llio.string_from buf in
    let msgs = Llio.list_from Llio.string_from buf in

    let next_id = match user_db # get next_id_key with
      | None -> 0l
      | Some v -> deserialize Llio.int32_be_from v
    in
    let new_next_id =
      List.fold_left
        (fun next_id msg ->
           user_db # put
             (log_prefix ^ (serialize Llio.int32_be_to next_id))
             (Some msg);
           Int32.succ next_id)
        next_id
        msgs in
    user_db # put
      next_id_key
      (Some (serialize Llio.int32_be_to new_next_id));
    None

let name = "log_32"

let make_update ~next_id_key ~log_prefix ~msgs =
  let payload =
    serialize
      (Llio.tuple3_to
         Llio.string_to
         Llio.string_to
         (Llio.list_to Llio.string_to))
      (next_id_key, log_prefix, msgs)
  in
  Update.Update.UserFunction (name, Some payload)

let register =
  let registered = ref false in
  (fun () ->
     if not !registered
     then begin
       registered := true;
       Registry.Registry.register name user_function
     end)
