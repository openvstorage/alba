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
    let conn_info = ["127.0.0.1"],8000 + (Int64.to_int osd_id), false, false
    and asd_id = "asd id choose test " ^ (Int64.to_string osd_id) in
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
                 ~seen:[] ~read:[] ~write:[]
                 ~errors:[] ~checksum_errors:0L ~claimed_since:None
  in
  let () = Array.iteri
             (fun osd_id used ->
               let node_id = osd_id / 4 |> string_of_int in
               let osd_id = Int64.of_int osd_id in
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
                         (Nsm_model.EncryptInfo.KeySha256 "xxx"));
      chunk_sizes = [39552];
      size = 39552L;
      checksum = Checksum.NoChecksum;
      fragment_locations = [[  (Some 0L, 0);
                               (Some 1L, 0);
                               (Some 2L, 0);
                               (* we can move from {0,1,2} to 3 *)

                               (Some 4L, 0);
                               (Some 5L, 0);
                               (Some 6L, 0);
                               (Some 7L, 0);

                               (Some 8L, 0);
                               (Some 9L, 0);
                               (Some 10L, 0);
                               (Some 11L, 0);

                               (Some 12L, 0);
                               ]];
      fragment_checksums = [[Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum;
                             Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum;
                             Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum; Checksum.NoChecksum;]
                           ];
      fragment_packed_sizes = [[4960; 4960; 4960; 4960;
                                4960; 4960; 4960; 4960;
                                4960; 4960; 4960; 4960]
                              ];
      fragment_ctrs = [];
      version_id = 2;
      max_disks_per_node = 3; timestamp = 1457402732.57
    }
  in
  let categories = calculate_fill_rates cache |> categorize in
  let r = plan_move cache categories manifest in
  match r with
    | None -> OUnit.assert_equal true false ~msg:"there should be a solution"
    | Some (m,src,tgt,_score) ->
       Printf.printf "src:%Li -> tgt:%Li\n" src tgt


let suite =
  let open OUnit in
  "Rebalancing_helper" >::: ["check_move" >:: test_check_move]
