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

module Layout = struct
    type 'a t = 'a list list [@@deriving show, yojson]

    let map_indexed f t =
      List.mapi
        (fun ci c ->
          List.mapi (fun fi fr -> f ci fi fr) c
        ) t

    let output a_to buf t =
      let ser_version = 1 in Llio.int8_to buf ser_version;
      Llio.list_to (Llio.list_to a_to) buf t
    let input a_from buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      Llio.list_from (Llio.list_from a_from) buf

    let map f t = List.map (List.map f) t

    let map2 f t0 t1 = List.map2 (List.map2 f) t0 t1

    let map4 f x_t y_t z_t u_t = List.map4 (List.map4 f) x_t y_t z_t u_t

    let congruent x_t y_t =
      let list_ok x y = List.length x = List.length y in
      let rec ok = function
        | [],[] -> true
        | x :: xs, y::ys -> list_ok x y && ok (xs,ys)
        | _,_ -> false
      in
      ok (x_t,y_t)

    let index t chunk_id fragment_id =
      List.nth_exn (List.nth_exn t chunk_id) fragment_id

    let unfold ~n_chunks ~n_fragments f =
      List.unfold
        (fun chunk_id ->
          if chunk_id = n_chunks
          then None
          else
            Some
              (List.unfold
                 (fun fragment_id ->
                   if fragment_id = n_fragments
                   then None
                   else Some (f chunk_id fragment_id, fragment_id + 1))
                 0,
               chunk_id + 1))
        0

    let fold f a0 t =
      List.fold_left
        (fun acc l0 ->
          List.fold_left (fun acc l1 -> f acc l1) acc l0
        ) a0 t
end
