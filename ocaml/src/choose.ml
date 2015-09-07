(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
*)

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
        | x ::  xt ->
           let w = weight_of x in
           if n < cw +. w
           then x, List.rev_append sx xt
           else loop (cw +. w) (x :: sx) xt
        | _ -> failwith "can't happen"
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
          (bags : Int32.t node list) =
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

open Albamgr_protocol.Protocol

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
  let open Osd in

  let per_node = Hashtbl.create 47 in
  let node_ids = ref StringSet.empty in
  Hashtbl.iter
    (fun d_id d_info ->
     let node_id = d_info.node_id in
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
            let tf = Int64.to_float info.total in
            let uf = Int64.to_float info.used in
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
  let _state2s = [%show : int32 Inner.node_bag list] in
  let state0 = build_initial_state info in
  assert (Hashtbl.length info >= n);

  let state1_u = List.fold_left Inner.choose_device_forced state0 chosen in

  (* sort bags, as choose_devices expects this *)
  let state1 =
    let count_of = function
      | Inner.G(w,c,_):: _ -> c
      | _ -> failwith "count_of"
    in
    let compare_bags x y = count_of x - count_of y
    in
    List.sort compare_bags state1_u in

  let r_inner = Inner.choose_devices n state1 in
  List.map
    (function
      | Inner.D (_, _, osd_id) ->
        (osd_id, Hashtbl.find info osd_id)
      | Inner.G (_,_,_) -> failwith "can't happen 3"

    )
    r_inner

let choose_devices n info : (Osd.id * Osd.t ) list =
  choose_extra_devices n info []
