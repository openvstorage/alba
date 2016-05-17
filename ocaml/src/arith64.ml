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

type operation =
  | PLUS_EQ

let operation_of = function
  | 1 -> PLUS_EQ
  | _ -> failwith "arith64:no such operation"

let name = "arith64"

let make_update key operation x=
  match operation with
  | PLUS_EQ ->
     begin
     let buf = Buffer.create 20 in
     Llio.int8_to buf 1;
     Llio.string_to buf key;
     Llio.int64_to buf x;
     let payload = Buffer.contents buf in
     Update.Update.UserFunction(name, Some payload)
     end

let user_function (user_db : Registry.user_db) (value_o: string option)
    : string option
  =
  let inner payload =
    let buf = Llio.make_buffer payload 0 in
     let operation_i = Llio.int8_from buf in
     let operation = operation_of operation_i in
     begin
       match operation with
       | PLUS_EQ ->
          let key = Llio.string_from buf in
          let value = Llio.int64_from buf in
          let a_so = user_db # get key in
          let a =
            match a_so with
            | None -> 0L
            | Some a_v -> Prelude.deserialize Llio.int64_from a_v
          in
          let a' = Int64.add a value in
          let r =
            if a' = 0L
            then None
            else Some (Prelude.serialize Llio.int64_to a')
          in
          user_db # put key r;
          r
     end
  in
  match value_o with
  | None -> None
  | Some value -> inner value

let register =
  let registered = ref false in
  (fun () ->
   if not !registered
   then
     begin
       registered:= true;
       Registry.Registry.register name user_function
     end
  )
