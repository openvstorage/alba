(*
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
