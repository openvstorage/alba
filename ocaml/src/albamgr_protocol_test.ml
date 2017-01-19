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

open Albamgr_protocol

let make_info () =
  let open Nsm_model.OsdInfo in
  let ts = [4.;3.;2.;1.;0.] in
  let kind = Asd(([],8000, false, false ),"asd") in
    make
      ~kind
      ~node_id:"node_id" ~other:"" ~total:100L ~used:0L
      ~decommissioned:false
      ~seen:ts
      ~read:ts
      ~write:ts
      ~errors:[]
      ~checksum_errors:3L
      ~claimed_since:(Some 5.0)

let create seen checksum_errors=
  Protocol.Osd.Update.make
    ~ips':[] ~port':0 ~used':10L ~total':99L ~seen':seen
    ~checksum_errors':checksum_errors ()

let test_apply () =
  let open Nsm_model.OsdInfo in
  let info = make_info() in
  let u1 = create [6.1;5.1;4.1;3.1;2.1;1.1] (Some 2L) in
  let kind = Asd(([], 0, false, false),"asd") in
  let r = Protocol.Osd.Update.apply info u1 in
  let expected =
    make
      ~kind
      ~node_id:"node_id" ~other:"" ~total:99L ~used:10L
      ~decommissioned:false
      ~seen:[6.1;5.1;4.1;4.0;3.1;3.0;2.1;2.0;1.1;1.0]
      ~read:info.read
      ~write:info.write
      ~errors:[]
      ~checksum_errors:5L
      ~claimed_since:(Some 5.0)
  in
  OUnit.assert_equal ~printer:[%show : t] expected r ;
  ()


let test_update_serialization () =
  let update = create [6.1;5.1;4.1;3.1;2.1;1.1] (Some 2L) in
  let module U = Protocol.Osd.Update in
  let buf = Buffer.create 128 in
  let () = U.to_buffer buf update ~version:2 in
  let ser = Buffer.contents buf in
  let buf2 = Llio.make_buffer ser 0 in
  let update' = U.from_buffer buf2 in
  OUnit.assert_equal ~printer:[%show :U.t] update update';
  ()

let suite =
  let open OUnit in
  "albamgr_protocol" >::: [
      "test_apply" >:: test_apply;
      "test_update_serialization" >:: test_update_serialization;
    ]
