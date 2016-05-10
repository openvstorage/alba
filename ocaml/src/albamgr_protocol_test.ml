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

open Albamgr_protocol

let test_apply () =
  let open Nsm_model.OsdInfo in
  let create seen =
    Protocol.Osd.Update.make ~ips':[] ~port':0 ~used':10L ~total':99L ~seen':seen ()
  in
  let ts = [4.;3.;2.;1.;0.] in
  let kind = Asd(([],8000, false),"asd") in
  let info =
    make
      ~kind
      ~node_id:"node_id" ~failure_domains:[ "x", "y"; ]
      ~other:"" ~total:100L ~used:0L
      ~decommissioned:false
      ~seen:ts
      ~read:ts
      ~write:ts
      ~errors:[]

  in
  let u1 = create [6.1;5.1;4.1;3.1;2.1;1.1] in
  let r = Protocol.Osd.Update.apply info u1 in
  let kind = Asd(([], 0, false),"asd") in
  let expected =
    make
      ~kind
      ~node_id:"node_id" ~failure_domains:[ "x", "y"; ]
      ~other:"" ~total:99L ~used:10L
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
