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

open Lwt.Infix

open Albamgr_protocol.Protocol
open Alba_test
open Prelude
open Nsm_model

let test_replication_read_preference () =
  let test_name = "test_replication_read_preference" in
  let namespace = test_name in
  let preset_name = test_name in
  let node_ids, prefered, others =
    Albamgr_test.test_with_albamgr
      (fun mgr ->
        mgr # list_all_claimed_osds >>= fun (_,osds) ->
        let all_nodes =
          let per_node =
            List.group_by
              (fun (_,osd_info) -> osd_info.OsdInfo.node_id)
              osds
          in
          Hashtbl.to_assoc_list per_node
          |> List.sort (fun (k,_) (k2,_) -> String.compare k k2)
        in
        match all_nodes with
        | (nid0,n0s):: (nid1, n1s) :: rest ->
           let prefered = List.hd_exn n0s |> fst in
           let others =
             List.map fst n1s
           in
         Lwt.return ([nid0], prefered, others)
        | _ -> failwith "not enough nodes?"
      )
  in
  test_with_alba_client
    ~read_preference:node_ids
    (fun alba_client ->
      Lwt_log.info_f "preference for: %s prefered:%Li others:%s"
                     ([%show : string list] node_ids)
                     prefered
                     ([%show: int64 list] others)
      >>= fun () ->
      let open Preset in
      let preset =
        { _DEFAULT with
          policies = [(1, 4, 5, 4); ];
          osds = Explicit ([prefered] @ others);
        }
      in
      let get_stats osd_id =
        alba_client
          # osd_access # get_osd_info ~osd_id
        >>= fun (info,_state,_caps) ->
        let open OsdInfo in
        match info.kind with
        | Asd (conn_info,_) ->
           let conn_info =
             Asd_client.conn_info_from
               ~tls_config:None conn_info
           in
           Asd_client.with_client
             ~conn_info None
             (fun client ->
               client # statistics true >>= fun stats ->
               Lwt.return (osd_id, stats)
             )
        | _ -> failwith "setup changed?"

      in
      let check_node_ids = [prefered;8L] in
      Lwt_list.map_p get_stats check_node_ids >>= fun _ ->

      alba_client # mgr_access # create_preset preset_name preset
      >>= fun () ->
      alba_client # create_namespace
                  ~namespace ~preset_name:(Some preset_name) ()
      >>= fun namespace_id ->
      _wait_for_osds ~cnt:4 alba_client namespace_id >>= fun () ->

      let object_data =
        Lwt_bytes.of_string (Prelude.get_random_string 100)
      in
      let object_name = test_name in
      alba_client # upload_object_from_bytes
                  ~namespace
                  ~object_name
                  ~object_data
                  ~checksum_o:None
                  ~allow_overwrite:NoPrevious
      >>= fun (mf,_,_,_) ->
      Lwt_io.printlf "mf:%s" (Manifest.show mf) >>= fun () ->
      Lwt_list.iter_p
      (fun _ ->
        alba_client # download_object_to_string
                    ~namespace
                    ~object_name
                    ~consistent_read:true
                    ~should_cache:false
        >>= fun data_o ->
        Lwt.return ()
      ) [1;2;3;4;5]

      >>= fun () ->
      Lwt_list.map_p get_stats check_node_ids >>= fun stats ->

      let open Statistics_collection.Generic in
      let open Asd_protocol.Protocol in
      let get_n_multiget2 coll =
        let stat =
          _find_stat coll
                     (command_to_code (Wrap_query MultiGet2))
        in stat.n
      in
      Lwt_list.iter_s
        (fun (osd_id, collection ) ->

          let count = get_n_multiget2 collection in
          Lwt_io.printlf
            "%Li:%s => %Li"
            osd_id
            (show_inner
               collection code_to_description)
            count
        ) stats
      >>= fun () ->
      let counts =
        List.map
          (fun (osd_id,coll) -> get_n_multiget2 coll)
          stats
      in
      let max = List.hd_exn counts in
      let rest = List.tl_exn counts in
      Lwt_log.info_f "max:%Li rest:%S" max ([%show: int64 list] rest)

      >>= fun () ->
      List.iter
        (fun c ->
          OUnit.assert_bool
            "nonprefered osd used too much"
            (Int64.add c 2L < max)
        )
        rest;
      Lwt.return_unit
    )


open OUnit
let suite = "read_preference" >:::[
      "test_replication_read_preference" >:: test_replication_read_preference;
    ]
