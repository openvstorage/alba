open Albamgr_protocol

let test_apply () =
  let open Protocol.Osd in
  let create seen =
    Update.make ~ips':[] ~port':0 ~used':10L ~total':99L ~seen':seen ()
  in
  let ts = [4.;3.;2.;1.;0.] in
  let kind = Asd(([],8000, false),"asd") in
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
  let r = Update.apply info u1 in
  let kind = Asd(([], 0, false),"asd") in
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
