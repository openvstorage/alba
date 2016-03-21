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

module Lwt_bytes = struct
  include Lwt_bytes

  let show (t:t) =
    let l = Lwt_bytes.length t in
    if l < 32
    then Printf.sprintf "<Lwt_bytes: length=%i %S>" l (to_string t)
    else Printf.sprintf "<Lwt_bytes: length=%i _ >" l


  let pp formatter t =
    Format.pp_print_string formatter (show t)

  let unsafe_destroy (t : t) = Core_kernel.Bigstring.unsafe_destroy t

  let create_random size =
    let r = Lwt_bytes.create size in
    for i = 0 to size - 1 do
      Lwt_bytes.unsafe_set r i (Random.int 256 |> Char.chr)
    done;
    r
end
