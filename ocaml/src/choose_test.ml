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

open Prelude
open Albamgr_protocol.Protocol

type r_t = int * ((Osd.id * Nsm_model.OsdInfo.t ) list)

open Choose

let _TOTAL = Int64.shift_left 1L 42
let _USED  = Int64.shift_left 1L 41

let test () =
  let info = Hashtbl.create 47 in
  Random.init 42;
  let pop = 32 in
  let rec fill i =
      if i = pop
      then ()
      else
        let device_id = Int32.of_int i in
        let node_id = string_of_int (i lsr 2) in
        let kind = Nsm_model.OsdInfo.Asd (
                       (["127.0.0.1"], 8000 +i, false),
                       "asd id choose test " ^ (string_of_int i)
                     )
        in
        let d_info =
          Nsm_model.OsdInfo.make
            ~node_id ~kind
            ~decommissioned:false
            ~other:""
            ~total:_TOTAL
            ~used:_USED
            ~seen:[]
            ~read:[]
            ~write:[]
            ~errors:[]
        in
        let () = Hashtbl.add info device_id d_info in
        fill (i+1)
  in
  let () = fill 0 in
  let distribution = Array.make pop 0 in
  let rec loop j =
    if j = 1000
    then ()
    else
      let r = Choose.choose_devices 12 info in
      let all =
        List.fold_left
          (fun acc (did,_) -> Int32Set.add did acc)
          Int32Set.empty r
      in
      let () = OUnit.assert_equal
                 ~msg:"they all need to be different"
                 (Int32Set.cardinal all) 12
      in
      let () =
        Int32Set.iter
          (fun i32 ->
           let i = Int32.to_int i32 in
           let c0 = distribution.(i) in
           let () = distribution.(i) <- c0 + 1 in
           ()
          ) all
      in
      loop (j+1)
  in
  let () = loop 0 in
  Printf.printf "%s\n" ([%show : int array] distribution )


let choose_bug () =
  let pop = 12 in
  let info = Hashtbl.create 13 in
  let open Nsm_model in
  let rec fill i =
    if i = pop
    then ()
    else
      let device_id = Int32.of_int i in
      let node_id = "my node" in
      let conn_info = ["127.0.0.1"], 8000 + i, false in

      let kind = OsdInfo.Asd(conn_info, "asd id choose bug " ^ string_of_int i) in
      let d_info =
        OsdInfo.make
          ~node_id ~kind ~decommissioned:false ~other:""
          ~total:_TOTAL
          ~used:_USED
          ~seen:[]
          ~read:[]
          ~write:[]
          ~errors:[]
      in
      let () = Hashtbl.add info device_id d_info in
      fill (i+1)
  in
  let () = fill 0 in
  let r = Choose.choose_devices 12 info in
  let () = Printf.printf "r=%s\n" ([%show : (Osd.id * OsdInfo.t ) list ] r) in
  ()


let choose_forced () =
  let info = Hashtbl.create 47 in
  Random.self_init ();
  let seed = Random.int 1_073_741_823 in
  Printf.printf "seed = %i" seed;
  Random.init seed;
  let pop = 32 in
  let open Nsm_model in
  let rec fill i =
      if i = pop
      then ()
      else
        let osd_id = Int32.of_int i in
        let node_id = string_of_int (i lsr 2) in
        let conn_info = (["127.0.0.1"], 8000 +i, false) in
        let kind = OsdInfo.Asd (conn_info,
                                          "osd id choose forced test " ^ (string_of_int i) )
        in
        let d_info =
          OsdInfo.make
            ~node_id ~kind
            ~decommissioned:false ~other:""
            ~total:_TOTAL ~used:_USED
            ~seen:[]
            ~read:[]
            ~write:[]
            ~errors:[]
        in
        let () = Hashtbl.add info osd_id d_info in
        fill (i+1)
  in
  let () = fill 0 in
  let chosen_osd_ids = [0l; 1l; 4l; 5l;] in
  let rec test = function
    | 0 -> ()
    | n ->
      let r = Choose.choose_extra_devices 1 info chosen_osd_ids in
      let extra_osd_id, _ = List.hd_exn r in
      assert (extra_osd_id > 7l);
      test (n - 1)
  in
  test 10_000

let test_bias () =
  let xws = [(0,0.1);
             (1,0.1);
             (2,0.7)]
  in
  let counts = Array.make 3 0 in
  let rec loop i =
    if i = 0
    then ()
    else
      let xw, rest = Inner.take_biased snd xws in
      let x = fst xw in
      let () = counts.(x) <- counts.(x) + 1 in
      loop (i-1)
  in
  let n = 15000 in
  let () = loop n in
  let total = List.fold_left (fun s (x,w) -> s +. w) 0.0 xws in
  Array.iteri
    (fun i c ->
     let cf = float c in
     let wi = snd (List.nth_exn xws i) in
     let measured = cf/. (float n) in
     let wanted = wi /. total in
     let x = (measured /. wanted) in
     Printf.printf "%i:measured:%f wanted:%f => %f\n" i measured wanted  x;
     OUnit.assert_bool "above 0.9" (x > 0.88);
     OUnit.assert_bool "below 1.1" (x < 1.12);
    )
    counts

let test_bias2 () =
  let weights = [0.05;0.05; 0.1;0.1;0.1;0.6] in
  let items   = [0;1;2;3;4;5] in
  let xws = List.combine items weights in
  let counts = Array.make 6 0 in
  let pick2 () =
    let xw0, r0 = Inner.take_biased snd xws in
    let xw1, r1 = Inner.take_biased snd r0 in
    let x0 = fst xw0 and x1 = fst xw1 in
    counts.(x0) <- counts.(x0) + 1;
    counts.(x1) <- counts.(x1) + 1
  in
  let n = 50000 in
  let rec loop i =
    if i = 0
    then ()
    else
      let () = pick2 () in
      loop (i-1)
  in
  loop n;
  let total = List.fold_left (+.) 0.0 weights in
  Array.iteri
    (fun i c ->
     let cf = float c in
     let wi = List.nth_exn weights i in
     let measured = cf/. (float n) in
     let wanted = wi /. total in
     let x = (measured /. wanted) in
     Printf.printf "%i:measured:%f wanted:%f => %f\n" i measured wanted  x;
     ()
    )
    counts

let test_actually_rebalances () =
  let open Inner in
  let () = Random.init 42 in
  let n_nodes = 5 in
  let node_size = 4 in
  let n = n_nodes * node_size in
  let distribution = Array.make n 0 in
  let total = 400_000.0 in

  let used =
    Array.init
      n
      (fun i -> (float (i / 4)) *. 100.0)
  in
  let pos_of node_id i = node_id * node_size + i in
  let make_node node_id =
      List.map
      (fun i ->
       let p = pos_of node_id i in
       let w = total /. (1.0 +. used.(p)) in
       D(w, 0, (node_id,i))
      )
      [0;1;2;3]
  in
  let build_state () =
    let rec loop acc i =
      if i = n_nodes
      then List.rev acc
      else
        let node = make_node i in
        let w = group_weight node in
        let a = G(w, 0, [node]) in
        loop (a :: acc) (i+1) in
    let groups = loop [] 0 in
    [groups]
  in
  let fa2s a= ([%show: float array] a) in
  let calc_criterium () =
    let total = Array.fold_left (+.) 0.0 used in
    let n_inv = 1.0 /. (float (Array.length used)) in
    let avg = total *. n_inv in
    let s = Array.fold_left
              (fun acc u ->
               let d = u -. avg in
               let d2 = d *. d in
               acc +. d2) 0.0 used
    in let sigma = sqrt(s *. n_inv) in
       sigma /. (0.1 +. avg)
  in


  let n_steps = 20_000 in

  let rec simulation crit j =
    let () =
      if j mod 1000 = 0
      then Printf.printf "%3i:%s\n" j (fa2s used)
    in
    if j = n_steps
    then ()
    else
      begin
      let state0 = build_state () in
      let state1 = Inner.choose_devices 12 state0 in
      let () =
        List.iter
        (function
          | D(_,_,(node_id,i)) ->
             let p = pos_of node_id i in
             let () = distribution.(p) <- distribution.(p) + 1 in
             let () = used.(p) <- used.(p) +. 1.0 in
             ()
          | G(_,_,_) -> failwith "should not happen"
        )
        state1
        in
        let crit' =
          if j mod 1000 = 0
          then calc_criterium ()
          else crit
        in
        let () =
          if j mod 500 = 0
          then
            let msg = Printf.sprintf "crit:%f crit':%f\n" crit crit' in
            OUnit.assert_bool msg (crit' <= crit)
        in
        simulation crit' (j+ 1)
      end
  in

  let () = simulation (calc_criterium ()) 0 in
  let () = Printf.printf "used:\n%s\n" (fa2s used) in
  let () = Printf.printf "distro:\n%s\n" ([%show : int array] distribution) in
  let crit = calc_criterium () in
  Printf.printf "crit = %f\n" crit;

  ()

let setup_explicit_info info_list =
  let open Nsm_model in
  let make_kind osd_id =
    let conn_info = ["127.0.0.1"],8000 + (Int32.to_int osd_id), false
    and asd_id = "asd id choose test " ^ (Int32.to_string osd_id)
    in
    OsdInfo.Asd (conn_info, asd_id)
  in
  let info = Hashtbl.create 15 in
  let () =
    List.iter
      (fun (osd_id,node_id) ->
       let d_info =
         OsdInfo.make
           ~node_id
           ~kind:(make_kind osd_id)
           ~decommissioned:false
           ~other:""
           ~total:_TOTAL ~used:_USED
           ~seen:[]
           ~read:[]
           ~write:[]
           ~errors:[]
       in
       Hashtbl.add info osd_id d_info) info_list
  in
  info

let test_choose_extra_bug () =
  let n = 1
  and info_list = [
  ( 0l, "2000");( 1l, "2001");( 2l, "2001");( 3l, "2000");
  ( 4l, "2000");( 5l, "2001");( 6l, "2001");( 7l, "2002");
  ( 8l, "2000");( 9l, "2002");(10l, "2002");(11l, "2002");
  ]
  and chosen = [10l; 2l; 0l; 9l; 4l; 6l; 1l]
  in
  let info = setup_explicit_info info_list in
  let r = choose_extra_devices n info chosen in
  let osd_ids = List.map fst r in
  Printf.printf "osd_ids:%s\n%!" ([%show : int32 list] osd_ids);
  ()

let test_choose_extra_bug2() =
  let n = 1
  and info_list = [
      (6l, "2001");  (10l, "2002"); ( 3l, "2000"); ( 0l, "2000");
      ( 1l, "2000"); ( 4l, "2001"); ( 9l, "2002"); (11l, "2002");
      ( 7l, "2001"); ( 8l, "2002"); ( 2l, "2000"); ( 5l, "2001")
  ]
  and chosen = [3l;7l;9l;0l;4l;1l;5l;8l;] in
  let info = setup_explicit_info info_list in
  let r = choose_extra_devices n info chosen in
  let osd_ids = List.map fst r in
  let osds_per_node = Hashtbl.create 16 in
  let open Nsm_model.OsdInfo in
  let () =
    List.iter
      (fun osd_id ->
       let osd_info = Hashtbl.find info osd_id in
       let node_id = osd_info.node_id in
       let cnt =
         try Hashtbl.find osds_per_node node_id
         with Not_found -> 0
       in
       let cnt' = cnt + 1 in
       Hashtbl.replace osds_per_node node_id cnt'
      )
      chosen
  in
  let chosen_list =
    Hashtbl.fold
      (fun node_id cnt acc -> (node_id,cnt) :: acc) osds_per_node []
  in
  Printf.printf "chosen_list:%s\n" ([%show : (string * int) list] chosen_list);
  Printf.printf "osd_ids:%s\n%!" ([%show : int32 list] osd_ids);
  let extra_id,node_id =
    let (extra_id, osd) = List.hd_exn r in
    let node_id = osd.node_id in
    (extra_id, node_id)
  in
  let count' = (List.assoc node_id chosen_list) + 1 in
  Printf.printf
    "%li on node:%s => %i osds on that node\n"
    extra_id node_id count';
  OUnit.assert_bool "too many osds on node" (count' < 4);
  ()

let suite =
  let open OUnit in
  ["choose" >:: test;
   "choose_bug" >:: choose_bug;
   "choose_extra_bug" >:: test_choose_extra_bug;
   "choose_extra_bug2">:: test_choose_extra_bug2;
   "choose_forced" >:: choose_forced;
   "bias1" >:: test_bias;
   "bias2" >:: test_bias2;
   "actually_rebalances" >:: test_actually_rebalances;

  ]
