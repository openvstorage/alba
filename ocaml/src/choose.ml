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

exception EmptyChoose

module Inner = struct

    let take_random items =
      let size = List.length items in
      let n = Random.int size in
      let rec loop acc xs i =
        match xs with
        | xh :: xt ->
           if i = n
           then
             xh, List.rev_append acc xt
           else
             loop (xh ::acc) xt (i+1)
        | _ ->  failwith "can't happen"

      in
      loop [] items 0

    let take_biased weight_of xs =
      (*
         TODO:
         this is correct if you take 1 from the list.
         If you recurse, the deeper ones will be less and less correct.
         (but it's not too bad)
       *)
      let total =
        List.fold_left
          (fun sum n ->
           let w = weight_of n in
           sum +. w ) 0.0 xs
      in
      let n = Random.float total in
      let rec loop cw sx xs =
        match xs with
        | [] -> failwith "take_biased on empty list"
        | [x] -> x, List.rev sx (* don't fail with negative weights *)
        | x ::  xt ->
           let w = weight_of x in
           if n < cw +. w
           then x, List.rev_append sx xt
           else loop (cw +. w) (x :: sx) xt
      in
      loop 0.0 [] xs

    type count = int
      [@@deriving show]

    type 'a node =
      | D of float * count * 'a
      | G of float * count * 'a node_bag list
      [@@deriving show]
    and 'b node_bag = 'b node list
      [@@deriving show]

    let count_of = function
      | D (_,c,_) -> c
      | G (_,c,_) -> c

    let inc = function
      | D (w, c, x) -> D(w, c+1, x)
      | G (w, c, x) -> G(w, c+1, x)

    let weight_of = function
      | D (w,_,_) -> w
      | G (w,_,_) -> w

    let insert (node:'a node) (node_bags:'a node_bag list) : 'a node_bag list =
      let cn = count_of node in
      let rec loop  acc = function
        | []      -> List.rev ([node] :: acc)
        | [] :: t -> loop acc t
        | ((item:: _) as h) :: t ->
           let c = count_of item in
           if c = cn
           then List.rev_append acc ((node :: h) :: t)
           else loop (h :: acc) t
      in
      loop [] node_bags

    let flatten node_bags =
      let rec loop acc = function
        | [] -> List.rev acc
        | [] :: t -> loop acc t
        | h :: t -> loop (h :: acc) t
      in
      loop [] node_bags

    let rec choose_device bags =
      match bags with
      | [] -> raise EmptyChoose
      | [] :: t -> choose_device t
      | bag :: t ->
         let p, rbags = take_biased weight_of bag in
         let p1r, t' = match p with
           | D (_, c,  _) ->
             p, t
           | G (w, c, xs) ->
              let x, xs' = choose_device xs in
              let t' =
                if xs' = []
                then t
                else
                  let p1u = G (w,c+1, xs') in
                  insert p1u t
              in
              x, t'
         in
         p1r , flatten (rbags :: t')

    let rec choose_device_forced
        (bags : 'a node_bag list)
        (osd_id : 'a) =
      let rec contains_osd_id : 'a node_bag -> bool = function
        | [] -> false
        | hd::tl ->
           let found_here =
             match hd with
             | D (_,_, (osd_id')) ->
              osd_id' = osd_id
            | G (_, _, xs) ->
              List.exists (fun bag -> contains_osd_id bag) xs
           in
           found_here || contains_osd_id tl
      in
      let take_osd_id
          (bags : Int64.t node list) =
        let rec inner acc = function
          | [] -> failwith "can't happen 2"
          | xs::tl ->
            if contains_osd_id [xs]
            then xs, List.rev_append acc tl
            else inner (xs::acc) tl
        in
        inner [] bags
      in
      let rec inner = function
        | [] -> failwith "can't happen 1"
        | h :: t ->
           let contains = contains_osd_id h in
           if contains
           then
             begin
               let p, hr = take_osd_id h in
               let t' = match p with
                 | D _ -> t
                 | G (w, c, xs ) ->
                    let xs' = choose_device_forced xs osd_id in
                    if xs' = []
                    then t
                    else insert (G (w, c+1, xs')) t
               in
               hr :: t'
             end
           else h ::  inner t
      in
      flatten (inner bags)

    let choose_devices n bags =
      let rec loop acc state i =
        if i = 0
        then acc
        else
          let d, state' = choose_device state in
          loop (d::acc) state' (i-1)
      in
      loop [] bags n
end

open Nsm_model

module StringSet = Set.Make(String)

let group_weight d_nodes =
  let sum,tot =
    List.fold_left
      (fun (sum,tot) d->
       sum +. Inner.weight_of d, tot + 1
      ) (0.0,0) d_nodes
  in
  sum /. (float tot)

let build_initial_state info =

  let per_node = Hashtbl.create 47 in
  let node_ids = ref StringSet.empty in
  Hashtbl.iter
    (fun d_id d_info ->
     let node_id = d_info.OsdInfo.node_id in
     Hashtbl.add per_node node_id (d_id,d_info);
     node_ids := StringSet.add node_id !node_ids
    ) info;

  let state0 =
    let open Inner in
    StringSet.fold
      (fun node_id acc ->
       let ds = Hashtbl.find_all per_node node_id in
       let d_nodes =
         List.map
           (fun (osd_id, info) ->
            let tf = Int64.to_float info.OsdInfo.total in
            let uf = Int64.to_float info.OsdInfo.used in
            let weight = tf /. (1.0 +. uf) in
            D(weight, 0, osd_id))
           ds
       in
       let w = group_weight d_nodes in
       let g = G (w, 0, [d_nodes]) in
       [g] :: acc
      ) !node_ids []
  in
  state0

let choose_extra_devices n info chosen =
  assert (Hashtbl.length info >= n);
  let _state2s = [%show : int64 Inner.node_bag list] in
  let state0 = build_initial_state info in

  let state1_u = List.fold_left Inner.choose_device_forced state0 chosen in

  (* sort bags, as choose_devices expects this *)
  let count_of = function
      | Inner.G(w,c,_):: _ -> c
      | _ -> failwith "count_of"
  in
  let state1 =
    let compare_bags x y = count_of x - count_of y in
    List.sort compare_bags state1_u
  in
  (* merge bags with same count *)
  let state2 =
    match state1 with
  | [] -> failwith "merge?"
  | bag0 :: bags ->
     let rec loop acc current_bag current_count=
       function
       | [] -> List.rev (current_bag::acc)
       | bag :: bags ->
          let count = count_of bag in
          if count = current_count
          then
            loop acc (current_bag @ bag) count bags
          else
            loop (current_bag :: acc) bag count bags
     in
     loop [] bag0 (count_of bag0) bags
  in
  (*let () = Printf.printf "state2:%s\n" (_state2s state2) in*)

  let r_inner = Inner.choose_devices n state2 in
  List.map
    (function
      | Inner.D (_, _, osd_id) ->
        (osd_id, Hashtbl.find info osd_id)
      | Inner.G (_,_,_) -> failwith "can't happen 3"

    )
    r_inner

let choose_devices n info : (Albamgr_protocol.Protocol.Osd.id * OsdInfo.t ) list =

  choose_extra_devices n info []
