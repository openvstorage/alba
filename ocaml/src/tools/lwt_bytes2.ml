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

module Lwt_bytes = struct
  include Lwt_bytes

  let show (t:t) =
    let l = Lwt_bytes.length t in
    if l < 60
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

module SharedBuffer = struct
  type t = { b : Lwt_bytes.t ; mutable ref_count : int}

  let make_shared b = { b ; ref_count = 1}

  let create len = Lwt_bytes.create len |> make_shared

  let length t = Lwt_bytes.length t.b

  let register_sharing t = t.ref_count <- t.ref_count + 1
  let unregister_usage t =
    let ref_count' = t.ref_count -1 in
    let () = t.ref_count <- ref_count' in
    if ref_count' = 0
    then
      Lwt_bytes.unsafe_destroy t.b

  let deref t = t.b

  let of_string s = Lwt_bytes.of_string s |> make_shared

  let to_string t = Lwt_bytes.to_string t.b
end
