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

module OsdCapabilities = struct
  type capability =
    | CMultiGet2
    | CPartialGet
    | CRoraFetcher of int                         (* port *)
    | CRoraFetcher2 of string list * int * string (* ips * port * transport *)
  [@@deriving show]

  let capability_to buf c =
    let module L = Llio2.WriteBuffer in
    let write_version version =
      L.int8_to buf version
    in
    match c with
    | CMultiGet2     -> write_version 1;
                        L.int8_to buf 1
    | CPartialGet    -> write_version 1;
                        L.int8_to buf 2
    | CRoraFetcher p -> write_version 1;
                        L.int8_to buf 3;
                        L.int_to buf p
    | CRoraFetcher2 (ips, port, transport) ->
       write_version 2;
       L.serialize_with_length
         buf
         (fun buf () ->
          L.int8_to buf 4;
          L.list_to L.string_to buf ips;
          L.int_to buf port;
          L.string_to buf transport)
         ()

  let capability_from buf =
    let module L = Llio2.ReadBuffer in
    let inner buf =
      let tag = L.int8_from buf in
      match tag with
      | 1 -> `Ok CMultiGet2
      | 2 -> `Ok CPartialGet
      | 3 -> let port = L.int_from buf in
             `Ok (CRoraFetcher port)
      | 4 ->
         let ips = L.list_from L.string_from buf in
         let port = L.int_from buf in
         let transport = L.string_from buf in
         `Ok (CRoraFetcher2 (ips, port, transport))
      | k -> `BadTag k
    in
    let version = L.int8_from buf in
    match version with
    | 1 ->
       begin
         match inner buf with
         | `Ok x -> `Ok x
         | `BadTag k -> raise_bad_tag "asd_capability" k
       end
    | 2 ->
      L.deserialize_length_prefixed
        inner
        buf
    | k -> raise_bad_tag "OsdCapabilities/version" k


  type t = capability Prelude.counted_list [@@deriving show]
  let to_buffer   = Llio2.WriteBuffer.counted_list_to  capability_to
  let from_buffer buf =
    let (_, caps) = Llio2.ReadBuffer.counted_list_from capability_from buf in
    let caps' =
      List.map_filter
        (function
          | `Ok cap -> Some cap
          | `BadTag _ -> None)
        caps
    in
    List.length caps', caps'

  let deser = from_buffer,to_buffer
  let default = (0,[])

end
