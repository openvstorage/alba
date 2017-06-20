(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

type fill_rate = int * (int32 * float) list [@@deriving show]


let calculate_fill_rates cache =
  let open Nsm_model in
  let rates = Hashtbl.fold
    (fun osd_id osd acc ->
     let open OsdInfo in
     let fill_rate = Int64.to_float osd.used /. Int64.to_float osd.total in
     let r = (osd_id, fill_rate) in
     r :: acc)
    cache []
  in
  (Hashtbl.length cache, rates)

let calculate_avg (n,fill_rates) =
  let total =
    List.fold_left (fun t (_, p) -> (t +. p)) 0. fill_rates
  in
  total /. float n

let calculate_var avg (n,fill_rates) =
  let sum =
    List.fold_left
      (fun acc (osd, fr) ->
       let delta = avg -. fr in
       acc +. (delta *. delta)
      ) 0. fill_rates
  in
  sum /. float n

let categorize fill_rates =
  let avg = calculate_avg fill_rates in
  let var = calculate_var avg fill_rates in
  let sigma = sqrt var in
  let n_sigmas = 1.0 in
  let step = 0.01 +. n_sigmas *. sigma in
  let lo = avg -. step in
  let hi = avg +. step in
  let (++) (n,xs) x = (n+1, x::xs) in
  List.fold_left
    (fun acc x ->
     let too_low, ok, too_high = acc
     and _,fr = x
     in
     let acc' =
       if fr < lo      then too_low ++ x,      ok, too_high
       else if fr < hi then too_low     , ok ++ x, too_high
       else                 too_low     ,      ok, too_high ++ x
     in
     acc'
    )
    ((0,[]), (0,[]), (0,[]))
    (snd fill_rates)


let compare_moves a b=
  let score (_,_,_,x) =  x in
  (score a) - (score b)

let compare_moves' a b = compare_moves b a

let plan_move cache (too_low,ok, too_high) manifest
    (*:  (manifest * osd_id * osd_id * int) option *) =
  let open Nsm_model in
  let as_set (n,xs) =
    List.fold_left
      (fun acc (osd_id,_) -> DeviceSet.add osd_id acc)
      DeviceSet.empty xs
  in
  let too_low_set = as_set too_low
  and ok_set = as_set ok
  and too_high_set = as_set too_high
  in
  let fill_rate_of =
    let tbl = Hashtbl.create 16 in
    let add (osd_id,f) = Hashtbl.replace tbl osd_id f in
    let () = List.iter add (snd too_high) in
    let () = List.iter add (snd ok) in
    let () = List.iter add (snd too_low) in
    fun osd_id -> Hashtbl.find tbl osd_id
  in
  let object_osds = Manifest.osds_used manifest in
  let node_id_of osd_id =
    let open Nsm_model.OsdInfo in
    let info = Hashtbl.find cache osd_id in
    info.node_id
  in
  let osds_used_on_node =
    let h = Hashtbl.create 16 in
    let () =
      DeviceSet.iter
        (fun osd_id ->
         let node_id = node_id_of osd_id in
         let count = try Hashtbl.find h node_id with Not_found -> 0 in
         Hashtbl.replace h node_id (count + 1)
        ) object_osds
    in
    fun node_id -> Hashtbl.find h node_id
  in
  let check_move src tgt =
    let tgt_nid = node_id_of tgt
    and src_nid = node_id_of src
    in
    src_nid = tgt_nid
    || osds_used_on_node tgt_nid < manifest.Manifest.max_disks_per_node
  in

  let valid_moves sources targets =
    DeviceSet.fold
      (fun src acc ->
       DeviceSet.fold
         (fun tgt acc ->
          if check_move src tgt
          then
            let score =
              (((fill_rate_of src) -. (fill_rate_of tgt) ) *. 1000.0
              |> int_of_float)
              + if (node_id_of src = node_id_of tgt) then 10 else 0
            in
            (manifest, src, tgt, score) :: acc
          else
            acc
         )
         targets acc
      ) sources []
  in
  let search have (sources,targets) =
    match have with
    | Some x -> have
    | None ->
       let vm = valid_moves sources targets in
       let sorted = List.sort compare_moves' vm in
       List.hd sorted
  in
  let high_src = DeviceSet.inter object_osds too_high_set in
  let ok_src   = DeviceSet.inter object_osds ok_set in
  let ok_tgt   = DeviceSet.diff ok_set object_osds in
  let low_tgt  = DeviceSet.diff too_low_set object_osds in
  List.fold_left
    search
    None
    [(high_src, low_tgt);
     (high_src, ok_tgt);
     (ok_src  ,low_tgt)
    ]


 let get_some_manifests
       (alba_client:Alba_base_client.client)
       ~make_first_last_reverse
       ~namespace_id
       osd_id
   =
   let open Lwt.Infix in
   let first, last, reverse = make_first_last_reverse() in
   alba_client # with_nsm_client' ~namespace_id
     (fun nsm_client ->
      nsm_client # list_device_objects
       ~osd_id
       ~first ~finc:true
       ~last  ~reverse
       ~max:100
     )
   >>= fun (r, has_more) ->
   Lwt.return r
