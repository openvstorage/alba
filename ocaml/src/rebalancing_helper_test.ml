(*
Copyright 2016 iNuron NV

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

let total = 1999419994112L 
let used =
  [|
    1973915885568L;
    1969058283520L;
    1962894372864L;
    1656622084096L;(* lo *)
    1630524301312L;(* lo *)
    1977804931072L;
    1979440607232L;
    1979444133888L;
    1894344544256L;
    1892951126016L;
    1893734473728L;
    1893670576128L;
    1894410674176L;
    1892949770240L;
    1894553870336L;
    1893062881280L
  |]

let test_check_move () =
  let open Nsm_model in
  let make_kind osd_id =
    let conn_info = ["127.0.0.1"],8000 + (Int32.to_int osd_id), false, false
    and asd_id = "asd id choose test " ^ (Int32.to_string osd_id) in
    OsdInfo.Asd (conn_info, asd_id)
  in
  let open Rebalancing_helper in
  let cache = Hashtbl.create 16 in
  let make_osd osd_id used node_id =
    let kind = make_kind osd_id in
    OsdInfo.make ~node_id
                 ~kind
                 ~decommissioned:false
                 ~other:""
                 ~total
                 ~used
                 ~seen:[] ~read:[] ~write:[] ~errors:[]
  in
  let () = Array.iteri
             (fun osd_id used ->
               let node_id = osd_id / 4 |> string_of_int in
               let osd_id = Int32.of_int osd_id in
               let osd = make_osd osd_id used node_id in
               Hashtbl.add cache osd_id osd)
             used
  in
  let manifest =
    { Nsm_model.Manifest.name = "xxx";
      object_id = "whatever";
      storage_scheme = Nsm_model.Storage_scheme.EncodeCompressEncrypt (
                           Nsm_model.Encoding_scheme.RSVM (8, 4,
                                                           Nsm_model.Encoding_scheme.W8),
                           Alba_compression.Compression.NoCompression);
      encrypt_info = Nsm_model.EncryptInfo.Encrypted (
                         Encryption.Encryption.AES (Encryption.Encryption.CBC,
                                                    Encryption.Encryption.L256),
                         (Nsm_model.EncryptInfo.KeySha1 "xxx"));
      chunk_sizes = [39552];
      size = 39552L;
      checksum = Checksum.NoChecksum;
      fragment_locations = [[  (Some 0l, 0);
                               (Some 1l, 0);
                               (Some 2l, 0);
                               (* we can move from {0,1,2} to 3 *)

                               (Some 4l, 0);
                               (Some 5l, 0);
                               (Some 6l, 0);
                               (Some 7l, 0);

                               (Some 8l, 0);
                               (Some 9l, 0);
                               (Some 10l, 0);
                               (Some 11l, 0);

                               (Some 12l, 0);
                               ]];
      fragment_checksums = [[Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum;
                             Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum;
                             Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum;]
                           ];
      fragment_packed_sizes = [[4960; 4960; 4960; 4960;
                                4960; 4960; 4960; 4960;
                                4960; 4960; 4960; 4960]
                              ];
      version_id = 2;
      max_disks_per_node = 3; timestamp = 1457402732.57
    }
  in
  let categories = calculate_fill_rates cache |> categorize in
  let r = plan_move cache categories manifest in
  match r with
    | None -> OUnit.assert_equal true false ~msg:"there should be a solution"
    | Some (m,src,tgt,_score) ->
       Printf.printf "src:%li -> tgt:%li\n" src tgt
  

let suite =
  let open OUnit in
  "Rebalancing_helper" >::: ["check_move" >:: test_check_move]
