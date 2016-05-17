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

let test_apply () =
  let open Nsm_model.OsdInfo in
  let create seen =
    Protocol.Osd.Update.make ~ips':[] ~port':0 ~used':10L ~total':99L ~seen':seen ()
  in
  let ts = [4.;3.;2.;1.;0.] in
  let kind = Asd(([],8000, false, false ),"asd") in
  let info =
    make
      ~kind
      ~node_id:"node_id" ~other:"" ~total:100L ~used:0L
      ~decommissioned:false
      ~seen:ts
      ~read:ts
      ~write:ts
      ~errors:[]

  in
  let u1 = create [6.1;5.1;4.1;3.1;2.1;1.1] in
  let r = Protocol.Osd.Update.apply info u1 in
  let kind = Asd(([], 0, false, false),"asd") in
  let expected =
    make
      ~kind
      ~node_id:"node_id" ~other:"" ~total:99L ~used:10L
      ~decommissioned:false
      ~seen:[6.1;5.1;4.1;4.0;3.1;3.0;2.1;2.0;1.1;1.0]
      ~read:ts
      ~write:ts
      ~errors:[]
  in
  OUnit.assert_equal ~printer:[%show : t] expected r ;
  ()


let suite =
  let open OUnit in
  "albamgr_protocol" >:::[
           "test_apply" >:: test_apply
         ]
