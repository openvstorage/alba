(*
Copyright 2015 Open vStorage NV

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
