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

let test_key = "test"
let bench_prefix = "@"

module AlbaInstanceRegistration = struct
  let next_alba_instance = "i"
  let instance_log = 'l'
  let instance_log_key id_on_osd =
    serialize
      (Llio.pair_to
         Llio.char_to
         Llio.int32_be_to)
      (instance_log,
       id_on_osd)

  let instance_content_prefix = 'p'
  let instance_index = 'j'
  let instance_index_key ~alba_id =
    serialize
      (Llio.pair_to
         Llio.char_to
         Llio.string_to)
      (instance_index,
       alba_id)
end

module AlbaInstance = struct
  let next_msg_id =
    serialize
      (Llio.tuple3_to
         Llio.char_to
         Llio.int32_be_to
         Llio.char_to)
      (AlbaInstanceRegistration.instance_content_prefix,
       0l,
       'm')

  let namespace_prefix_serializer buf namespace_id =
    Llio.char_to buf AlbaInstanceRegistration.instance_content_prefix;
    Llio.int32_be_to buf 0l;
    Llio.char_to buf 'n';
    Llio.int32_be_to buf namespace_id

  let namespace_status ~namespace_id =
    serialize namespace_prefix_serializer namespace_id

  let namespace_name ~namespace_id =
    serialize
      (Llio.pair_to
         namespace_prefix_serializer
         Llio.char_to)
      (namespace_id, 'n')

  let gc_epoch_tag
      ~namespace_id ~gc_epoch
      ~object_id ~version_id
      ~chunk_id ~fragment_id =
    serialize
      (fun buf () ->
         namespace_prefix_serializer buf namespace_id;
         Llio.char_to buf 'g';
         Llio.int64_be_to buf gc_epoch;
         Llio.string_to buf object_id;
         Llio.int_to buf chunk_id;
         Llio.int_to buf fragment_id;
         Llio.int_to buf version_id)
      ()

  let parse_gc_epoch_tag key =
    deserialize
      (fun buf ->
         let p = Llio.char_from buf in
         assert (p = 'p');
         let _0l = Llio.int32_be_from buf in
         assert (_0l = 0l);
         let n = Llio.char_from buf in
         assert (n = 'n');
         let namespace_id = Llio.int32_be_from buf in
         let g = Llio.char_from buf in
         assert (g = 'g');
         let gc_epoch = Llio.int64_be_from buf in
         let object_id = Llio.string_from buf in
         let chunk_id = Llio.int_from buf in
         let fragment_id = Llio.int_from buf in
         let version_id = Llio.int_from buf in
         (namespace_id, gc_epoch, object_id, chunk_id, fragment_id, version_id)
      )
      key

  let fragment
      ~namespace_id
      ~object_id ~version_id
      ~chunk_id ~fragment_id =
    serialize
      (fun buf () ->
         namespace_prefix_serializer buf namespace_id;
         Llio.char_to buf 'o';
         Llio.string_to buf object_id;
         Llio.int_to buf chunk_id;
         Llio.int_to buf fragment_id;
         Llio.int_to buf version_id)
      ()

  let fragment_recovery_info
      ~namespace_id
      ~object_id
      ~chunk_id ~fragment_id ~version_id =
    serialize
      (fun buf () ->
         namespace_prefix_serializer buf namespace_id;
         Llio.char_to buf 'r';
         Llio.string_to buf object_id;
         Llio.int_to buf chunk_id;
         Llio.int_to buf fragment_id;
         Llio.int_to buf version_id)
      ()

  let fragment_recovery_info_next_prefix
      ~namespace_id =
    serialize
      (fun buf () ->
         namespace_prefix_serializer buf namespace_id;
         Llio.char_to buf (Char.chr (Char.code 'r' + 1)))
      ()

  let parse_fragment_recovery_info key =
    deserialize
      (fun buf ->
         let p = Llio.char_from buf in
         assert (p = 'p');
         let _0l = Llio.int32_from buf in
         assert (_0l = 0l);
         let n = Llio.char_from buf in
         assert (n = 'n');
         let namespace_id = Llio.int32_from buf in
         let r = Llio.char_from buf in
         assert (r = 'r');
         let object_id = Llio.string_from buf in
         let chunk_id = Llio.int_from buf in
         let fragment_id = Llio.int_from buf in
         let version_id = Llio.int_from buf in
         (namespace_id, object_id, chunk_id, fragment_id, version_id))
      key
end
