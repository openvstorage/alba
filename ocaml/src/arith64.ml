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
