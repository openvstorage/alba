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
open Slice
open Encryption
open Lwt

let test_with_alba_client ?bad_fragment_callback f =
  let albamgr_client_cfg = Albamgr_test.get_ccfg () in
  Lwt_main.run begin
    Alba_client.with_client
      ?bad_fragment_callback
      (ref albamgr_client_cfg)
      ~release_resources:true
      f
  end

let _wait_for_osds ?(cnt=11) (alba_client:Alba_client.alba_client) namespace_id =
  alba_client # nsm_host_access # get_namespace_info ~namespace_id
  >>= fun (ns_info, _, _) ->
  let nsm_host_id = ns_info.Albamgr_protocol.Protocol.Namespace.nsm_host_id in
  let rec loop () =
    alba_client # deliver_nsm_host_messages ~nsm_host_id >>= fun () ->
    alba_client # nsm_host_access # refresh_namespace_osds ~namespace_id
    >>= fun (cnt', osd_ids) ->
    if cnt' >= cnt
    then Lwt.return ()
    else Lwt_unix.sleep 0.1 >>= fun () -> loop ()
  in
  loop ()

let delete_fragment
      (alba_client:Alba_client.alba_client)
      namespace_id object_id
      (osd_id, version_id)
      chunk_id fragment_id =
  alba_client # with_osd
    ~osd_id
    (fun osd ->
     osd # apply_sequence
         Osd.High
         []
         [ Osd.Update.delete_string
             (Osd_keys.AlbaInstance.fragment
                ~namespace_id
                ~object_id ~version_id
                ~chunk_id
                ~fragment_id) ]
     >>= fun s ->
     OUnit.assert_equal Osd.Ok s;
     Lwt.return ())

let maybe_delete_fragment
  ~update_manifest
  alba_client
  namespace_id manifest
  chunk_id fragment_id =
  let open Nsm_model in
  let osd_id_o, version_id =
    Layout.index
      manifest.Manifest.fragment_locations
      chunk_id fragment_id in
  match osd_id_o with
  | None ->
    Lwt.return ()
  | Some osd_id ->
    let object_id = manifest.Manifest.object_id in
    delete_fragment
      alba_client
      namespace_id object_id
      (osd_id, version_id)
      chunk_id fragment_id >>= fun () ->
    if update_manifest
    then
      alba_client # nsm_host_access # get_gc_epoch ~namespace_id >>= fun gc_epoch ->
      alba_client # with_nsm_client'
        ~namespace_id
        (fun client ->
           client # update_manifest
             ~object_name:manifest.Manifest.name
             ~object_id
             [ (chunk_id, fragment_id, None); ]
             ~gc_epoch ~version_id:(manifest.Manifest.version_id + 1))
    else
      Lwt.return ()

let get_checksum_o object_data =
  let checksum =
    let hasher = Hashes.make_hash Checksum.Checksum.Algo.CRC32c in
    hasher # update_string object_data;
    hasher # final ()
  in
  Some checksum


let safe_delete_namespace client namespace =
  Lwt.catch
    (fun () ->
       client # delete_namespace ~namespace)
    (fun exn ->
       Lwt_log.debug_f
         ~exn
         "Ignoring exception in delete namespace test cleanup")

let wait_for_work alba_client =
  let maintenance_client =
    new Maintenance.client
        ~retry_timeout:1.
        (alba_client # get_base_client)
  in
  maintenance_client # do_work ~once:true ()

let safe_decommission (alba_client : Alba_client.alba_client) long_ids =
  Lwt_list.iter_p
    (fun long_id ->
       Lwt.catch
         (fun () -> alba_client # decommission_osd ~long_id)
         (let open Albamgr_protocol.Protocol.Error in
          function
          | Albamgr_exn (Osd_already_decommissioned, _) -> Lwt.return ()
          | exn -> Lwt.fail exn))
    long_ids
  >>= fun () ->

  alba_client # mgr_access # list_all_nsm_hosts () >>= fun (_, nsm_hosts) ->

  let rec wait_no_more_decommissionings () =
    wait_for_work alba_client >>= fun () ->

    alba_client # mgr_access # list_decommissioning_osds
      ~first:0l ~finc:true ~last:None ~max:1 ~reverse:false >>= fun ((cnt, _), _) ->
    alba_client # mgr_access # get_work
      ~first:0l ~finc:true ~last:None ~max:1 ~reverse:false >>= fun ((cnt', _), _) ->
    if cnt + cnt' > 0
    then wait_no_more_decommissionings ()
    else Lwt.return ()
  in

  let rec deliver_nsm_host_messages () =
    Lwt_list.iter_p
      (fun (nsm_host_id, _, _) ->
       alba_client # deliver_nsm_host_messages ~nsm_host_id)
      nsm_hosts >>= fun () ->
    Lwt_unix.sleep 1. >>=
    deliver_nsm_host_messages
  in

  Lwt.pick
    [ wait_no_more_decommissionings ();
      deliver_nsm_host_messages (); ]


let test_upload_download () =
  test_with_alba_client
      (fun alba_client ->
         let namespace = "test_upload_download" in
         alba_client # create_namespace ~namespace ~preset_name:None ()
         >>= fun namespace_id ->

         let object_name = "obj_name" in
         let object_data =
           Lwt_bytes.of_string "a probably big string containing all data of this object." in
         Lwt_io.printlf "storing object with length=%i"
                       (Lwt_bytes.length object_data) >>= fun () ->

         let open Nsm_model in
         alba_client # upload_object_from_bytes
           ~namespace
           ~object_name
           ~object_data
           ~checksum_o:None
           ~allow_overwrite:NoPrevious
         >>= fun _ ->

         Lwt_io.printlf "stored object" >>= fun () ->

         let verify_download () =
           alba_client # download_object_to_string
                       ~namespace
                       ~object_name
                       ~consistent_read:true
                       ~should_cache:false
           >>= fun res ->
           let object_data_s' = Option.get_some res in
           let object_data_s  = Lwt_bytes.to_string object_data in
           let ok = object_data_s = object_data_s' in
           Lwt_io.printlf "Download result = %s ; length=%i; equal expected=%b"
                          object_data_s'
                          (String.length object_data_s')
                          ok >>= fun () ->
           assert ok;
           Lwt.return ()
         in

         verify_download () >>= fun () ->

         (* sabotage a few devices, containing both data & coding fragments *)
         alba_client # get_object_manifest
           ~namespace
           ~object_name
           ~consistent_read:true
           ~should_cache:false
         >>= fun (hm,omanifest) ->
         let manifest = Option.get_some omanifest in
         let locations = List.hd_exn (manifest.Manifest.fragment_locations) in

         maybe_delete_fragment
           ~update_manifest:false
           alba_client namespace_id manifest
           0 0 >>= fun () ->
         maybe_delete_fragment
           ~update_manifest:false
           alba_client namespace_id manifest
           0 (List.length locations - 1) >>= fun () ->

         verify_download () >>= fun () ->

         Lwt.return ())

let test_delete_namespace () =
  test_with_alba_client
      (fun client ->
         let namespace = "test_delete_namespace" in
         client # create_namespace ~namespace ~preset_name:None ~nsm_host_id:"ricky" ()
         >>= fun namespace_id ->
         Lwt_io.printlf "created namespace with id %li" namespace_id >>= fun () ->

         let objs = ["1"; "2"; "a"; "b"] in
         Lwt_list.iter_p
           (fun name ->
              let open Nsm_model in
              client # upload_object_from_bytes
                ~namespace
                ~object_name:name
                ~object_data:(Lwt_bytes.of_string name)
                ~checksum_o:None
                ~allow_overwrite:NoPrevious >>= fun _mf ->
                Lwt.return ()
           )
           objs >>= fun () ->
         client # delete_object ~namespace ~object_name:"a" ~may_not_exist:false >>= fun () ->

         let assert_nsm_host_prefix prefix assertion =
           Lwt_io.printlf "asserting about nsm_host" >>= fun () ->
           Client_helper.with_master_client'
             ~tls:None
             (Albamgr_protocol.Protocol.Arakoon_config.to_arakoon_client_cfg
                (Albamgr_test.get_ccfg ()))
             (fun client ->
                client # prefix_keys prefix (-1)
                >>= fun keys ->
                assert (assertion keys);
                Lwt_io.printlf "done asserting about nsm_host") in
         let assert_nsm_host_info assertion =
           assert_nsm_host_prefix
             (Nsm_host_plugin.Keys.namespace_info namespace_id)
             assertion in
         let assert_nsm_host_content assertion =
           assert_nsm_host_prefix
             (Nsm_host_plugin.Keys.namespace_content namespace_id)
             assertion in

         client # get_object_manifest
           ~namespace ~object_name:"1"
           ~consistent_read:true
           ~should_cache:false
         >>= fun (hm,manifest_o) ->
         let manifest = Option.get_some manifest_o in
         let object_id = manifest.Nsm_model.Manifest.object_id in
         let locations =
           let open Nsm_model in
           manifest.Manifest.fragment_locations
         in
         let osd_id_o, version_id = List.hd_exn (List.hd_exn locations) in
         let osd_id = Option.get_some osd_id_o in
         Lwt_io.printlf "using osd %li for assertions" osd_id >>= fun () ->
         let assert_osd presence fragment =
           client # with_osd ~osd_id
             (fun c ->
                let open Osd_keys in
                let open Slice in
                c # get_option
                  Osd.High
                  (wrap_string (AlbaInstance.namespace_status ~namespace_id)) >>= fun ps ->
                let p = Option.map Lwt_bytes.to_string ps in
                Lwt_io.printlf "got p = %s" ([%show : string option] p) >>= fun () ->
                assert (presence = (p <> None));
                let fragment_key = wrap_string
                                     (AlbaInstance.fragment
                                        ~namespace_id
                                        ~object_id ~version_id
                                        ~chunk_id:0
                                        ~fragment_id:0
                                     )
                in
                c # get_option Osd.High fragment_key >>= fun fs ->
                let f = Option.map Lwt_bytes.to_string fs in
                Lwt_io.printlf "got f = %s" ([%show : string option] f) >>= fun () ->
                assert (fragment = (f <> None));
                Lwt.return ()) in

         assert_osd true true >>= fun () ->
         assert_nsm_host_info ((<>) []) >>= fun () ->
         assert_nsm_host_content ((<>) []) >>= fun () ->

         client # delete_namespace ~namespace >>= fun () ->

         assert_osd true true >>= fun () ->
         assert_nsm_host_info ((=) []) >>= fun () ->
         assert_nsm_host_content ((<>) []) >>= fun () ->

         wait_for_work client >>= fun () ->

         assert_osd false false >>= fun () ->
         assert_nsm_host_info ((=) []) >>= fun () ->
         assert_nsm_host_content ((=) []) >>= fun () ->

         Lwt.return ())

let test_clean_obsolete_keys () =
  test_with_alba_client
      (fun client ->

         let maintenance_client = new Maintenance.client (client # get_base_client) in

         let namespace = "test_clean_obsolete_keys" in
         client # create_namespace ~preset_name:None ~namespace ~nsm_host_id:"ricky" ()
         >>= fun namespace_id ->

         let open Nsm_model in
         let object_name = "my object" in
         client # upload_object_from_bytes
           ~namespace
           ~object_name
           ~object_data:(Lwt_bytes.of_string "bla")
           ~checksum_o:None
           ~allow_overwrite:NoPrevious >>= fun _mf ->

         client # get_object_manifest ~namespace
                ~object_name
                ~consistent_read:true
                ~should_cache:false
         >>= fun (hm,manifest_o) ->
         let manifest = Option.get_some manifest_o in
         let object_id = manifest.Manifest.object_id in

         let locations = manifest.Manifest.fragment_locations in
         let osd_id_o_first_fragment, version_id = List.hd_exn (List.hd_exn locations) in
         let osd_id_first_fragment = Option.get_some osd_id_o_first_fragment in
         let assert_fragment assert_ =
           Lwt_io.printlf "assert_fragment.." >>= fun () ->
           client # with_osd
             ~osd_id:osd_id_first_fragment
             (fun osd_client ->
                let fragment_key =
                  Slice.wrap_string
                    (Osd_keys.AlbaInstance.fragment
                       ~namespace_id
                       ~object_id ~version_id
                       ~chunk_id:0
                       ~fragment_id:0)
                in
                osd_client # get_option Osd.High fragment_key >>= fun data_o ->
                assert (assert_ data_o);
                Lwt.return ()) in

         assert_fragment ((<>) None) >>= fun () ->

         client # delete_object
                ~namespace ~object_name ~may_not_exist:false
         >>= fun () ->

         assert_fragment ((<>) None) >>= fun () ->

         Lwt_io.printlf "cleaning obsolete fragments.." >>= fun () ->

         maintenance_client # clean_obsolete_keys_namespace ~once:true ~namespace_id >>= fun () ->

         assert_fragment ((=) None) >>= fun () ->

         Lwt.return ())

let test_garbage_collect () =
  test_with_alba_client
      (fun client ->
         let namespace = "test_garbage_collect" in
         client # create_namespace ~preset_name:None ~namespace ~nsm_host_id:"ricky" ()
         >>= fun namespace_id ->

         client # with_nsm_client'
           ~namespace_id
           (fun nsm ->
              nsm # list_all_active_osds) >>= fun (_, osd_ids) ->
         let osd_id = List.hd_exn osd_ids in

         let chunk_id = 0
         and fragment_id = 0
         and object_id = "bla bla"
         and version_id = 9 in
         Alba_client_upload.upload_packed_fragment_data
           (client # osd_access)
           ~namespace_id
           ~packed_fragment:(Bigstring_slice.create 1)
           ~osd_id ~version_id
           ~object_id
           ~chunk_id ~fragment_id
           ~gc_epoch:0L
           ~checksum:Checksum.Checksum.NoChecksum
           ~recovery_info_blob:(Osd.Blob.Bytes "")
         >>= fun _ ->

         let assert_fragment assert_ =
           Printf.printf "assert_fragment..\n";
           client # with_osd
             ~osd_id
             (fun osd_client ->
              let fragment_key =
                Slice.wrap_string
                  (Osd_keys.AlbaInstance.fragment
                     ~namespace_id
                     ~object_id ~version_id
                     ~chunk_id
                     ~fragment_id)
              in
                osd_client # get_option Osd.High fragment_key
                >>= fun data_o ->
                assert (assert_ data_o);
                Lwt.return ()) in

         assert_fragment ((<>) None) >>= fun () ->

         let maintenance_client = new Maintenance.client (client # get_base_client) in

         Printf.printf "garbage collecting fragments..\n";

         maintenance_client # garbage_collect_namespace
           ~once:true
           ~namespace_id
           ~grace_period:0. >>= fun () ->

         assert_fragment ((=) None) >>= fun () ->

         Lwt.return ())

let test_create_namespaces () =
  test_with_alba_client
      (fun client ->

         let rec inner = function
           | 300 -> Lwt.return ()
           | n ->
             let namespace = string_of_int n in
             Lwt_log.debug_f "creating namespace %s" namespace >>= fun () ->
             client # create_namespace ~preset_name:None ~namespace ~nsm_host_id:"ricky" () >>= fun namespace_id ->
             Lwt_log.debug_f "created namespace %s with id %li" namespace namespace_id >>= fun () ->

             let object_name = "bla" in
             let open Nsm_model in
             client # upload_object_from_bytes
               ~namespace
               ~object_name
               ~object_data:(Lwt_bytes.of_string "fds")
               ~checksum_o:None
               ~allow_overwrite:NoPrevious >>= fun _mf ->

             Lwt_log.debug_f "upload object to namespace %s succeeded, now deleting" namespace >>= fun () ->
             client # delete_object ~namespace ~object_name ~may_not_exist:false >>= fun () ->
             Lwt_log.debug_f "deleted object of namespace %s" namespace >>= fun () ->
             client # delete_namespace ~namespace >>= fun () ->
             Lwt_log.debug_f "deleted namespace %s" namespace >>= fun () ->

             let maintenance_client = new Maintenance.client (client # get_base_client) in
             maintenance_client # do_work ~once:true () >>= fun () ->

             Lwt_log.debug_f "finished work" >>= fun () ->

             inner (n + 1)
         in
         inner 0)

let test_partial_download () =
  test_with_alba_client
    (fun client ->
       let namespace = "test_partial_download" in
       client # create_namespace
         ~preset_name:None
         ~namespace () >>= fun namespace_id ->

       let object_name = "" in

       let object_data = "jfsdaovovvovo" in
       let object_data_ba = Lwt_bytes.of_string object_data in
       client # upload_object_from_bytes
         ~namespace
         ~object_name
         ~object_data:object_data_ba
         ~checksum_o:None
         ~allow_overwrite:Nsm_model.NoPrevious >>= fun _ ->

       let length = Bytes.length object_data in

       client # download_object_slices_to_string
         ~namespace
         ~object_name
         ~object_slices:[0L, length]
         ~consistent_read:true
       >>= fun res ->
       let object_data' = Option.get_some res in
       assert (object_data = object_data');

       client # upload_object_from_file
         ~namespace
         ~object_name
         ~input_file:"bin/kinetic-all-0.8.0.4-SNAPSHOT-jar-with-dependencies.jar"
         ~checksum_o:None
         ~allow_overwrite:Nsm_model.Unconditionally >>= fun (mf, _) ->

       let open Nsm_model in
       let size, checksum =
         let open Manifest in
         assert (1 < List.length mf.chunk_sizes);
         mf.size, mf.checksum
       in
       assert (checksum <> Checksum.NoChecksum);

       client # download_object_slices_to_string
         ~namespace
         ~object_name
         ~object_slices:[0L, Int64.to_int size]
         ~consistent_read:true
       >>= fun res ->
       let res = Option.get_some res in
       let hasher = Hashes.make_hash (Checksum.algo_of checksum) in
       hasher # update_string res;
       assert (checksum = hasher # final ());

       let hasher = Hashes.make_hash (Checksum.algo_of checksum) in
       let slice_length = Int64.of_int (4096*8) in
       (* read entire file in slices *)
       let rec download_partials acc offset =
         let remaining = Int64.(sub size offset) in
         let length64, continue =
           if Int64.(remaining <: slice_length)
           then remaining, false
           else slice_length, true
         in
         let length = Int64.to_int length64 in
         client # download_object_slices
           ~namespace
           ~object_name
           ~object_slices:[offset, length]
           ~consistent_read:true
           (fun _dest_off src off len ->
              hasher # update_lwt_bytes src off len;
              Lwt.return ()) >>= fun _ ->
         let acc' = (offset, length)::acc in
         if continue
         then download_partials acc' Int64.(add offset (of_int length))
         else Lwt.return (List.rev acc')
       in
       download_partials [] 0L >>= fun object_slices ->

       assert (checksum = hasher # final ());
       Lwt_log.debug_f "download slices variant 1 succeeded" >>= fun () ->

       begin
         let prev_offset = ref (-1) in
         let hasher2 = Hashes.make_hash (Checksum.algo_of checksum) in
         client # download_object_slices
           ~namespace
           ~object_name
           ~object_slices
           ~consistent_read:true
           (fun dest_off src off len ->
              assert (dest_off > !prev_offset);
              prev_offset := dest_off;
              hasher2 # update_lwt_bytes src off len;
              Lwt.return ()) >>= fun _ ->
         assert (checksum = hasher2 # final ());
         Lwt.return ()
       end >>= fun () ->

       begin
         Lwt_log.debug_f
           "object_slices = %s"
           ([% show : (Int64.t * int) list] object_slices) >>= fun () ->
         client # download_object_slices_to_string
           ~namespace
           ~object_name
           ~object_slices
           ~consistent_read:true
         >>= fun res1 ->
         client # download_object_slices_to_string
           ~namespace
           ~object_name
           ~object_slices:(List.rev object_slices)
           ~consistent_read:true
         >>= fun res2 ->

         assert (res1 <> res2);
         let sl = Int64.to_int slice_length in
         let slice1 = Slice.make (Option.get_some res1) 0 sl in
         let slice2 =
           Slice.make
             (Option.get_some res2)
             (Int64.to_int size - sl)
             sl in
         assert (Slice.compare' slice1 slice2 = Compare.EQ);

         Lwt.return ()
       end)

let test_encryption () =
  test_with_alba_client
    (fun alba_client ->
       let open Albamgr_protocol.Protocol in
       let namespace = "test_encryption" in
       let preset_name = "enc_preset" in
       let algo = Encryption.(AES (CBC, L256)) in
       let key = get_random_string (Encryption.key_length algo) in
       let preset' = Preset.({ _DEFAULT with
                               compression = Alba_compression.Compression.NoCompression;
                               fragment_encryption = Encryption.(AlgoWithKey (algo, key)); }) in
       alba_client # mgr_access # create_preset preset_name preset' >>= fun () ->

       alba_client # create_namespace ~namespace ~preset_name:(Some preset_name) () >>= fun _ ->

       let object_name = "object_name" in
       let object_data = "fsafaivvio;zjviz;" in
       alba_client # upload_object_from_bytes
         ~namespace
         ~object_name
         ~object_data:(Lwt_bytes.of_bytes object_data)
         ~checksum_o:None
         ~allow_overwrite:Nsm_model.NoPrevious >>= fun _ ->

       alba_client # download_object_to_string
         ~namespace
         ~object_name
         ~consistent_read:true
         ~should_cache:true
       >>= fun data_o ->

       Lwt_log.debug_f "got back %s" ([%show : string option] data_o) >>= fun () ->

       OUnit.assert_equal data_o (Some object_data);

       let object_name' = "empty" in
       let object_data' = "" in
       alba_client # upload_object_from_bytes
         ~namespace
         ~object_name:object_name'
         ~object_data:(Lwt_bytes.of_bytes object_data')
         ~checksum_o:None
         ~allow_overwrite:Nsm_model.NoPrevious >>= fun _ ->

       alba_client # download_object_to_string
         ~namespace
         ~object_name:object_name'
         ~consistent_read:true
         ~should_cache:true
       >>= fun data_o' ->

       Lwt_log.debug_f "got back2 %s" ([%show : string option] data_o') >>= fun () ->

       OUnit.assert_equal data_o' (Some object_data');

       Lwt.return ())


let test_discover_claimed () =
  let test_name = "test_discover_claimed" in
  test_with_alba_client
    (fun alba_client ->
       Asd_test.with_asd_client test_name 8230
         (fun asd ->
          alba_client # osd_access # seen
              ~check_claimed:(fun id -> true)
              ~check_claimed_delay:1.
              Discovery.(Good("", { id = test_name;
                                    extras = Some({
                                        node_id = "bla";
                                        version = "AsdV1";
                                        total = 1L;
                                        used = 1L;
                                      });
                                    ips = ["127.0.0.1"];
                                    port = 8230; })) >>= fun () ->

            let is_osd_available () =
              alba_client # mgr_access # list_available_osds
              >>= fun (_, available_osds) ->
              let res =
                List.exists
                  (fun osd ->
                     let open Albamgr_protocol.Protocol.Osd in
                     test_name = get_long_id osd.kind)
                  available_osds
              in
              Lwt.return res
            in

            (* check the osd is now 'available' *)
            is_osd_available () >>= fun r ->
            assert r;

            let module IRK = Osd_keys.AlbaInstanceRegistration in
            let open Slice in

            let alba_id = "lblfds" in

            let next_alba_instance' =
              wrap_string IRK.next_alba_instance
            in
            let no_checksum = Checksum.Checksum.NoChecksum in

            let id_on_osd = 0l in
            let instance_index_key = IRK.instance_index_key ~alba_id in
            let instance_log_key = IRK.instance_log_key id_on_osd in

            let open Osd in

            let osd = new Asd_client.asd_osd test_name asd in

            osd # apply_sequence
              Osd.High
              [ Assert.none next_alba_instance';
                Assert.none_string instance_log_key;
                Assert.none_string instance_index_key; ]
              [ Update.set
                  next_alba_instance'
                  (Osd.Blob.Bytes (serialize Llio.int32_to (Int32.succ id_on_osd)))
                  no_checksum true;
                Update.set_string
                  instance_log_key
                  alba_id
                  no_checksum true;
                Update.set_string
                  instance_index_key
                  (serialize Llio.int32_to id_on_osd)
                  no_checksum true; ]
            >>= fun apply_result ->
            OUnit.assert_equal Osd.Ok apply_result;

            (* this timeout should be enough... *)
            Lwt_unix.sleep 3. >>= fun () ->

            (* check the osd is no longer 'available' *)
            is_osd_available () >>= fun r ->
            assert (not r);

            Lwt.return ()
            ))

let test_change_osd_ip_port () =
  let test_name = "test_change_osd_ip_port" in
  let osd_name =
    test_name ^ Uuidm.(v4_gen (Random.State.make_self_init ()) () |> to_string) in

  let t (client : Alba_client.alba_client) =
       let open Albamgr_protocol.Protocol in
       let namespace = test_name in
       let object_name = "object_name" in

       Lwt.ignore_result
         (client # discover_osds
                 ~check_claimed:(fun id -> true)
                 ~check_claimed_delay:0.1 ());
       Lwt.ignore_result
         (client # osd_access # propagate_osd_info
                 ~delay:0.1 ());

       Asd_test.with_asd_client
         osd_name 16541
         (fun asd ->
            (* give maintenance process some time to discover the
               osd and register it in the albamgr *)
            Lwt_unix.sleep 0.2 >>= fun () ->
            client # osd_access # propagate_osd_info ~run_once:true () >>= fun () ->
            client # claim_osd ~long_id:osd_name >>= fun osd_id ->
            (* sleep a bit here so the alba client can discover it's claimed status *)
            Lwt_unix.sleep 0.2 >>= fun () ->

            client # mgr_access # create_preset
                   test_name
                   Preset.({
                       _DEFAULT with
                       policies = [(1,0,1,1)];
                       osds = Explicit [ osd_id ];
                     }) >>= fun () ->

            client # create_namespace ~preset_name:(Some test_name) ~namespace () >>= fun _ ->

            client # get_base_client # upload_object_from_string
              ~namespace
              ~object_name
              ~object_data:"fsdajivivivisjivo"
              ~allow_overwrite:Nsm_model.NoPrevious
              ~checksum_o:None >>= fun _ ->

            client # download_object_to_string
              ~namespace
              ~object_name
              ~consistent_read:true
              ~should_cache:true
            >>= function
            | Some _ -> Lwt.return ()
            | None -> Lwt.fail_with "downloading the object should succeed")
       >>= fun () ->

       (* TODO invalidate cache for namespace *)

       Lwt_unix.sleep 0.3 >>= fun () ->

       Asd_test.with_asd_client
         ~is_restart:true
         osd_name 16542
         (fun asd ->

            (* hmm or use with_osd !
               TODO, test with both... *)
            let download_obj () =
              client # download_object_to_string
                     ~namespace
                     ~object_name
                     ~consistent_read:true
                     ~should_cache:true >>= function
              | None -> assert false
              | Some _ -> Lwt.return ()
            in
            (* it's ok to fail once while clearing the borked
             * connection(s) *)
            let n = 2 in
            let rec loop i =
              if i = n
              then Lwt.fail_with "test failed... can't retrieve object"
              else
                Lwt_log.debug_f "attempt : %i" i >>= fun () ->
                Lwt.catch
                  (fun () -> download_obj ())
                  (let open Alba_client_errors.Error in
                   function
                   | Exn NotEnoughFragments ->
                      Lwt_log.debug "not enough fragments" >>= fun () ->
                      loop (i+1)
                   | exn -> Lwt_log.fatal ~exn "failing test"
                  )
            in
            loop 0
         )
  in
  test_with_alba_client
    (fun client ->
       Lwt.finalize
         (fun () ->
          t client >>= fun () ->
          Lwt_log.debug "==================== end")
         (fun () ->
            safe_delete_namespace client test_name >>= fun () ->
            Asd_test.with_asd_client
              ~is_restart:true
              osd_name 16543
              (fun _ ->
               client # osd_access # propagate_osd_info ~run_once:true () >>= fun () ->
               safe_decommission client [ osd_name ] >>= fun () ->
               wait_for_work client >>= fun () ->
               wait_for_work client)
         )
    )

let test_repair_by_policy () =
  let test_name = "test_repair_by_policy" in
  test_with_alba_client
    (fun alba_client ->
       let open Albamgr_protocol.Protocol in

       let preset_name = test_name in
       alba_client # mgr_access # create_preset
              preset_name
              Preset.({
                  _DEFAULT with
                  policies =
                    [ (2,1,2,3);
                      (1,0,1,1);
                    ];
                  (* only adding 1 disk so far... *)
                  osds = Explicit [ 0l ];
                }) >>= fun () ->

       let nsm_host_id = "ricky" in
       let namespace = test_name in
       alba_client # create_namespace
         ~preset_name:(Some preset_name)
         ~nsm_host_id
         ~namespace ()
       >>= fun namespace_id ->

       let get_k_m_x manifest =
         let open Nsm_model in
         let es = match manifest.Manifest.storage_scheme with
           | Storage_scheme.EncodeCompressEncrypt (es, _) -> es in
         let k, m, w = match es with
           | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
         k, m, manifest.Manifest.max_disks_per_node
       in

       let object_name = test_name in
       alba_client # get_base_client # upload_object_from_string
         ~namespace
         ~object_name
         ~object_data:"dfjsdl cjivo jiovppp"
         ~checksum_o:None
         ~allow_overwrite:Nsm_model.NoPrevious >>= fun (mf, _) ->

       assert ((1,0,1) = get_k_m_x mf);

       let new_osd_ids = [ 4l; 8l; 11l; ] in
       alba_client # mgr_access # add_osds_to_preset
         ~preset_name
         ~osd_ids:new_osd_ids >>= fun () ->

       Lwt_list.iter_p
         (fun osd_id -> alba_client # deliver_osd_messages ~osd_id)
         new_osd_ids >>= fun () ->

       alba_client # deliver_nsm_host_messages ~nsm_host_id >>= fun () ->

       alba_client # nsm_host_access # refresh_namespace_osds ~namespace_id >>= fun (cnt, ns_osds) ->

       Lwt_log.debug_f "%s"
         ([%show : Albamgr_protocol.Protocol.Osd.id list]
            ns_osds) >>= fun () ->

       assert (cnt = 4);

       let maintenance_client = new Maintenance.client (alba_client # get_base_client) in
       maintenance_client # repair_by_policy_namespace ~namespace_id >>= fun () ->

       alba_client # get_object_manifest
         ~namespace
         ~object_name
         ~consistent_read:true
         ~should_cache:true
       >>= fun (hm,mfo') ->
       let mf' = Option.get_some mfo' in

       assert ((2,1,3) = get_k_m_x mf');

       alba_client # get_base_client # with_nsm_client ~namespace
         (fun nsm ->
            nsm # list_objects_by_policy
              ~k:1 ~m:0
              ~max:100 >>= fun ((cnt, _),_) ->
            assert (cnt = 0);

            nsm # list_objects_by_policy
              ~k:2 ~m:1
              ~max:100 >>= fun ((cnt, _),_) ->
            assert (cnt = 1);

            Lwt.return ())
    )

let test_missing_corrupted_fragment () =
  let bad_fragment_callback
      (alba_client (* : Alba_client.alba_client *))
      ~namespace_id
      ~object_id ~object_name
      ~chunk_id ~fragment_id ~location =
    Lwt.ignore_result
      (Lwt_extra2.ignore_errors
         (fun () ->
            alba_client # mgr_access # add_work_repair_fragment
                   ~namespace_id ~object_id
                   ~object_name
                   ~chunk_id
                   ~fragment_id ~version_id:(snd location)))
  in
  test_with_alba_client
    ~bad_fragment_callback
    (fun alba_client ->
       let test_name = "test_missing_corrupted_fragment" in
       let namespace = test_name in

       let preset_name = test_name in
       alba_client # mgr_access # create_preset
         preset_name
         Albamgr_protocol.Protocol.Preset.({
             _DEFAULT with
             policies = [ (2,1,3,3); ];
           }) >>= fun () ->

       alba_client # create_namespace ~namespace ~preset_name:(Some preset_name) ()
       >>= fun namespace_id ->

       let object_name = test_name in
       let object_data = "fdi" in

       let open Nsm_model in

       alba_client # get_base_client # upload_object_from_string
         ~namespace
         ~object_name
         ~object_data
         ~checksum_o:None
         ~allow_overwrite:NoPrevious >>= fun (mf, _) ->

       let object_id = mf.Manifest.object_id in

       (* remove a fragment *)
       let locations = List.hd_exn (mf.Manifest.fragment_locations) in
       let victim_osd_o, version0 = List.hd_exn locations in
       let victim_osd = Option.get_some victim_osd_o in
       delete_fragment
         alba_client namespace_id object_id
         (victim_osd, 0)
         0 0
       >>= fun () ->

       (* downloading triggers repair  *)
       alba_client # download_object_to_string
         ~namespace
         ~object_name
         ~consistent_read:true
         ~should_cache:true
       >>= fun _ ->

       Lwt_unix.sleep 0.2 >>= fun () ->

       let maintenance_client = new Maintenance.client (alba_client # get_base_client) in
       maintenance_client # do_work ~once:true () >>= fun () ->

       alba_client # get_object_manifest
         ~namespace
         ~object_name
         ~consistent_read:true
         ~should_cache:true
       >>= fun (hm,r) ->
       let mf' = Option.get_some r in
       let locations' = List.hd_exn mf'.Manifest.fragment_locations in
       let l2s = [%show : (int32 option * int) list ] in
       Lwt_log.debug_f "locations :%s" (l2s locations ) >>= fun () ->
       Lwt_log.debug_f "locations':%s" (l2s locations') >>= fun () ->
       let (_,version1)  = List.hd_exn locations' in
       let ok = version0 < version1 in
       OUnit.assert_bool "version should be higher" ok;
       Lwt.return ())


let test_full_asd () =
  test_with_alba_client
    (fun alba_client ->
     let namespace = "test_full_asd" in
     alba_client # create_namespace ~namespace ~preset_name:None ()
     >>= fun namespace_id ->
     let object_name = "the_object"
     and object_data = Lwt_bytes.of_string "that'll do" in
     alba_client # upload_object_from_bytes
                 ~namespace
                 ~object_name
                 ~object_data
                 ~checksum_o:None
                 ~allow_overwrite:Nsm_model.NoPrevious
     >>= fun (mf,stats) ->
    (* all asd's are full *)
     let osd_ids =
       List.map
         Int32.of_int
         [0;1;2;3;4;5;6;7;8;9;10;11] in
     let set_full_all full =
       Lwt_list.iter_s
         (fun osd_id ->
          alba_client
            # with_osd ~osd_id
            (fun osd_client->  osd_client # set_full full)
         ) osd_ids
     in
     let inner () =
       (* download_still possible ? *)
       alba_client # download_object_to_string
                   ~namespace ~object_name
                   ~consistent_read:true
                   ~should_cache:true
       >>= fun res ->
       let ok = res <> None in
       OUnit.assert_bool "download should yield a blob" ok;
       (* delete_still possible ? *)
       alba_client # delete_object ~namespace ~object_name ~may_not_exist:false
       >>= fun () ->
       alba_client # delete_namespace ~namespace >>= fun () ->
       Lwt.catch
         (fun () ->
          Lwt_list.iter_s
            (fun osd_id ->
             Lwt_log.debug_f "delivering messages for %lil" osd_id
             >>= fun () ->
             alba_client # deliver_osd_messages ~osd_id)
            osd_ids
         )
         (fun exn ->
          let msg = Printf.sprintf "msg delivery failed:%s"
                                   (Printexc.to_string exn)
          in
          OUnit.assert_failure msg
         )
     (* remark: cleanup of fragments is ok, as it only contains deletes *)
     in
     set_full_all true >>= fun () ->
     Lwt.finalize
       (fun () -> inner ())
       (fun () -> set_full_all false)
    )


let test_versions () =
  test_with_alba_client
    (fun alba_client ->
     alba_client # mgr_access # get_version >>= fun mgr_version ->
     let nsm_host_id = "ricky" in
     let nsm = alba_client # nsm_host_access # get ~nsm_host_id in
     nsm # get_version >>= fun nsm_version ->
     alba_client # with_osd ~osd_id:0l
       (fun osd -> osd # get_version) >>= fun osd_version ->
     let printer (major,minor,patch,hash) =
       Printf.sprintf "(%i, %i, %i, %S)" major minor patch hash
     in
     Lwt_io.printlf "mgr_version:%s" (printer mgr_version) >>= fun () ->
     Lwt_io.printlf "nsm_version:%s" (printer mgr_version) >>= fun () ->
     Lwt_io.printlf "asd_version:%s" (printer osd_version) >>= fun () ->
     OUnit.assert_equal ~printer mgr_version nsm_version;
     OUnit.assert_equal ~printer mgr_version osd_version;
     Lwt.return ()

    )

let test_disk_churn () =
  let test_name = "test_disk_churn" in
  let preset_name = test_name in
  let namespace = test_name in
  let object_name = namespace in
  test_with_alba_client
    (fun alba_client ->

       let rec with_asds f acc = function
         | [] -> f acc
         | (asd_name, asd_port) :: asds ->
           Asd_test.with_asd_client asd_name asd_port
             (fun asd ->
                alba_client # osd_access # seen
                  ~check_claimed:(fun id -> true)
                  ~check_claimed_delay:1.
                  Discovery.(Good("", { id = asd_name;
                                        extras = Some({
                                            node_id = "bla";
                                            version = "AsdV1";
                                            total = 1L;
                                            used = 1L;
                                          });
                                        ips = ["127.0.0.1"];
                                        port = asd_port; })) >>= fun () ->
                alba_client # claim_osd ~long_id:asd_name >>= fun osd_id ->
                with_asds
                  f
                  (osd_id :: acc)
                  asds)
       in

       let asds = [
         ("test_disk_churn_0", 8240);
         ("test_disk_churn_1", 8241);
         ("test_disk_churn_2", 8242);
         ("test_disk_churn_3", 8243);
         ("test_disk_churn_4", 8244);
         ("test_disk_churn_5", 8245);
       ]
       in

       let fragment_size = 40 in
       let k = 2 in
       let object_size = fragment_size * k * 3 in
       let object_data = String.make object_size 'a' in

       let inner osd_ids =
         let preset =
           Albamgr_protocol.Protocol.Preset.({
               _DEFAULT
               with
                 osds = Explicit osd_ids;
                 policies = [ (k,1,k,k+1); (1,0,1,1); ];
                 fragment_size;
             })
         in

         alba_client # mgr_access # create_preset
           preset_name
           preset >>= fun () ->

         alba_client # create_namespace
           ~namespace
           ~preset_name:(Some preset_name) () >>= fun namespace_id ->

         _wait_for_osds ~cnt:6 alba_client namespace_id >>= fun () ->

         let open Nsm_model in

         alba_client # get_base_client # upload_object_from_string
           ~namespace
           ~object_name
           ~object_data
           ~checksum_o:None
           ~allow_overwrite:NoPrevious >>= fun (mf, _) ->

         let used_osds_set = Manifest.osds_used mf.Manifest.fragment_locations in
         let used_osds = DeviceSet.elements used_osds_set in
         assert (List.length used_osds = (k+1));

         Lwt_list.iter_s
           (fun osd_id ->
              alba_client # mgr_access # get_osd_by_osd_id ~osd_id >>= function
              | None -> Lwt.fail_with "can't find osd"
              | Some osd_info ->
                alba_client # decommission_osd
                  ~long_id:Albamgr_protocol.Protocol.Osd.(get_long_id osd_info.kind))
           used_osds
         >>= fun () ->

         alba_client # deliver_nsm_host_messages ~nsm_host_id:"ricky" >>= fun () ->

         let maintenance_client =
           new Maintenance.client
               ~retry_timeout:1.
               (alba_client # get_base_client) in

         Lwt_list.iter_s
           (fun osd_id ->
              maintenance_client # decommission_device
                ~deterministic:true
                ~namespace_id
                ~osd_id () >>= fun () ->

              maintenance_client # clean_obsolete_keys_namespace
                ~once:true ~namespace_id)
           used_osds
         >>= fun () ->

         maintenance_client # do_work ~once:true () >>= fun () ->

         alba_client # get_object_manifest
           ~consistent_read:true
           ~should_cache:false
           ~namespace
           ~object_name >>= fun (_, mf_o) ->

         assert (mf_o <> None);
         let mf' = Option.get_some mf_o in

         assert (mf'.Manifest.object_id = mf.Manifest.object_id);
         let used_osds_set' = Manifest.osds_used mf'.Manifest.fragment_locations in
         let used_osds' = DeviceSet.elements used_osds_set' in

         (* assert the decommissioned osds are no longer used *)
         List.iter
           (fun used_osd ->
              assert (not (List.mem used_osd used_osds')))
           used_osds;


         (* kill the fragments on the decommissioned osds explicitly *)
         Lwt_list.iter_p
           (fun (osd_id,fragment_id) ->
            delete_fragment
              alba_client
              namespace_id mf.Manifest.object_id
              (osd_id, 0)
              0 fragment_id
           )
           (List.mapi (fun i osd_id -> (osd_id, i)) used_osds)
         >>= fun () ->

         (* assert the used osds are only used once *)
         let rec inner = function
           | [] -> ()
           | osd_id :: osd_ids ->
             assert (not (List.mem osd_id osd_ids));
             inner osd_ids
         in
         inner used_osds';


         alba_client # download_object_to_string
           ~consistent_read:false
           ~should_cache:false
           ~namespace
           ~object_name >>= fun data_o ->

         assert (data_o <> None);

         begin
           Lwt_log.debug_f "starting disk churn test phase 2" >>= fun () ->
           (* decommission another osd. only 2 osds will remain.
            * the first policy (2,1,2,3) is still possible.
            * the object should not be rewritten.
            *)
           let osd_id = List.hd_exn used_osds' in

           alba_client # mgr_access # get_osd_by_osd_id ~osd_id >>=
             (function
               | None -> Lwt.fail_with "can't find osd"
               | Some osd_info ->
                  alba_client # decommission_osd
                              ~long_id:Albamgr_protocol.Protocol.Osd.(get_long_id osd_info.kind))
           >>= fun () ->
           maintenance_client # decommission_device
                              ~deterministic:true
                              ~namespace_id
                              ~osd_id () >>= fun () ->

           alba_client # get_object_manifest
                       ~consistent_read:true
                       ~should_cache:false
                       ~namespace
                       ~object_name >>= fun (_, mf_o) ->

           assert (mf_o <> None);
           let mf' = Option.get_some mf_o in

           (* TODO assert (mf'.Manifest.object_id = mf.Manifest.object_id); *)
           assert (2 = DeviceSet.cardinal (Manifest.osds_used mf'.Manifest.fragment_locations));
           Lwt.return ()
         end >>= fun () ->

         begin
           Lwt_log.debug_f "starting disk churn test phase 3" >>= fun () ->
           (* decommission another osd. now there will not be enough osds
            * remaining so object should be rewritten to (1,0,1,1)
            *)
           alba_client # get_object_manifest
                       ~consistent_read:true
                       ~should_cache:false
                       ~namespace
                       ~object_name >>= fun (_, mf_o) ->

           assert (mf_o <> None);
           let mf = Option.get_some mf_o in
           let used_osds =
             Manifest.osds_used mf.Manifest.fragment_locations
             |> DeviceSet.elements
           in

           let osd_id = List.hd_exn used_osds in

           alba_client # mgr_access # get_osd_by_osd_id ~osd_id >>=
             (function
               | None -> Lwt.fail_with "can't find osd"
               | Some osd_info ->
                  alba_client # decommission_osd
                              ~long_id:Albamgr_protocol.Protocol.Osd.(get_long_id osd_info.kind))
           >>= fun () ->
           maintenance_client # decommission_device
                              ~deterministic:true
                              ~namespace_id
                              ~osd_id () >>= fun () ->

           alba_client # get_object_manifest
                       ~consistent_read:true
                       ~should_cache:false
                       ~namespace
                       ~object_name >>= fun (_, mf_o) ->

           assert (mf_o <> None);
           let mf' = Option.get_some mf_o in

           assert (mf'.Manifest.object_id <> mf.Manifest.object_id);
           assert (1 = DeviceSet.cardinal (Manifest.osds_used mf'.Manifest.fragment_locations));

           Lwt.return ()
         end
       in

       with_asds
         (fun x ->
            Lwt.finalize
              (fun () -> inner x)
              (fun () ->
                 (* deleting the namespace so that the osds can be fully decommissioned *)
                 safe_delete_namespace alba_client namespace >>= fun () ->
                 safe_decommission
                   alba_client
                   (List.map fst asds)))
         []
         asds)

let test_replication () =
  let test_name = "test_replication" in
  test_with_alba_client
    (fun client ->

       let inner namespace =
         let object_name = test_name in
         let object_data = "a" in

         client # get_base_client # upload_object_from_string
           ~namespace
           ~object_name
           ~object_data
           ~checksum_o:None
           ~allow_overwrite:Nsm_model.NoPrevious
         >>= fun mf ->

         client # download_object_to_string
           ~namespace
           ~object_name
           ~consistent_read:true
           ~should_cache:false
         >>= fun data_o ->

         assert (data_o = Some object_data);

         client # download_object_slices_to_string
           ~namespace
           ~object_name
           ~object_slices:[ (0L, String.length object_data); ]
           ~consistent_read:true
         >>= fun data_o ->

         assert (data_o = Some object_data);

         Lwt.return ()
       in

       let open Albamgr_protocol.Protocol in

       begin
         let preset_name = test_name in

         let algo = Encryption.(AES (CBC, L256)) in
         let key = get_random_string (Encryption.key_length algo) in
         let preset' = Preset.({ _DEFAULT with
                                 policies = [(1,2,2,1);];
                                 fragment_encryption = Encryption.(AlgoWithKey (algo, key)); }) in
         client # mgr_access # create_preset preset_name preset' >>= fun () ->

         let namespace = test_name in

         client # create_namespace ~preset_name:(Some preset_name) ~namespace () >>= fun _ ->

         inner namespace
       end
       >>= fun () ->
       begin
         let preset_name = test_name ^ "2" in
         let preset' = Preset.({ _DEFAULT with
                                 compression = Alba_compression.Compression.NoCompression;
                                 policies = [(1,2,2,1);]; }) in
         client # mgr_access # create_preset preset_name preset' >>= fun () ->

         let namespace = test_name ^ "2" in

         client # create_namespace ~preset_name:(Some preset_name) ~namespace () >>= fun _ ->

         inner namespace >>= fun () ->

         (* check fragment size (should equal to object size) *)

         client # nsm_host_access # with_nsm_client ~namespace Lwt.return >>= fun nsm ->
         nsm # list_all_objects () >>= fun (_, objs) ->

         let object_name = List.hd_exn objs in
         nsm # get_object_manifest_by_name object_name >>= fun mf_o ->
         let mf = Option.get_some mf_o in

         let open Nsm_model in
         let open Manifest in
         let size = Int64.to_int mf.size in

         let fragment_size = List.hd_exn (List.hd_exn mf.fragment_packed_sizes) in

         Lwt_io.printlf "%s" (Manifest.show mf) >>= fun () ->

         assert (size = fragment_size);

         Lwt.return ()
       end
    )

let test_add_disk () =
  test_with_alba_client
    (fun alba_client ->
       let test_name = "test_add_disk" in
       let namespace = test_name in
       alba_client # create_namespace
         ~preset_name:None
         ~namespace () >>= fun namespace_id ->

       let asd_name = test_name in
       let asd_port = 17843 in
       Asd_test.with_asd_client asd_name asd_port
         (fun asd ->
            alba_client # osd_access # seen
              ~check_claimed:(fun id -> true)
              ~check_claimed_delay:1.
              Discovery.(Good("", { id = asd_name;
                                    extras = Some({
                                        node_id = "bla";
                                        version = "AsdV1";
                                        total = 1L;
                                        used = 1L;
                                      });
                                    ips = ["127.0.0.1"];
                                    port = asd_port; })) >>= fun () ->
            alba_client # claim_osd ~long_id:asd_name >>= fun osd_id ->

            alba_client # mgr_access # list_namespace_osds
              ~namespace_id
              ~first:osd_id ~finc:true ~last:(Some (osd_id, true))
              ~max:1 ~reverse:false >>= fun ((cnt, _), _) ->

            assert (cnt = 1);

            safe_decommission alba_client [ asd_name ]
         ))

let test_invalidate_deleted_namespace () =
  let test_name = "test_invalidate_deleted_namespace" in
  let namespace = test_name in
  test_with_alba_client
    (fun alba_client1 ->
       Alba_client.with_client
         ~release_resources:true
         (ref (Albamgr_test.get_ccfg ()))
         (fun alba_client2 ->

            (* alba_client1 is used to manipulate namespaces
               alba_client2 is tested for proper behaviour
               related to caching info about namespaces
            *)

            let object_data = "bla" in
            let checksum_o = get_checksum_o object_data in

            let do_upload (client : Alba_client.alba_client) =
              client # get_base_client # upload_object_from_string
                ~namespace
                ~object_name:""
                ~object_data
                ~checksum_o
                ~allow_overwrite:Nsm_model.Unconditionally >>= fun _ ->
              Lwt.return ()
            in

            alba_client1 # create_namespace ~namespace ~preset_name:None () >>= fun _ ->

            (* cache data about the namespace by doing an upload *)
            do_upload alba_client2 >>= fun () ->

            alba_client1 # delete_namespace ~namespace >>= fun () ->
            alba_client1 # create_namespace ~namespace ~preset_name:None () >>= fun _ ->

            do_upload alba_client2 >>= fun () ->

            alba_client1 # delete_namespace ~namespace >>= fun () ->
            alba_client1 # create_namespace ~namespace ~preset_name:None () >>= fun _ ->
            wait_for_work alba_client1 >>= fun () ->

            do_upload alba_client2 >>= fun () ->

            Lwt.return ()))


let test_master_switch () =
  let test_name = "test_master_switch" in
  let ccfg =
    Albamgr_test.get_ccfg()
    |> Albamgr_protocol.Protocol.Arakoon_config.to_arakoon_client_cfg in
  let rec wait_until_master () =
    let open Client_helper in
    find_master_loop ~tls:None ccfg
    >>= function
    | MasterLookupResult.Found (master, node_cfg) ->
      Lwt.return ()
    | _ ->
      Lwt_unix.sleep 0.2 >>=
      wait_until_master
  in
  let drop_master () =
    let open Client_helper in
    find_master_loop ~tls:None ccfg
    >>= function
    | MasterLookupResult.Found (master, node_cfg) ->
       begin
         Lwt_log.debug_f "master:%s" master >>= fun () ->
         let open Arakoon_client_config in
         let ip = List.hd_exn (node_cfg.ips) in
         let port = node_cfg.port in
         let sa = Networking2.make_address ip port in
         Lwt_io.with_connection
           sa
           (fun conn ->
            Lwt_log.debug_f "dropping master (can take a while)"
            >>= fun () ->
            let cluster = ccfg.cluster_id in
            Protocol_common.prologue cluster conn >>= fun () ->
            Protocol_common.drop_master conn >>= fun () ->
            Lwt_log.debug_f "dropped master call returned"
           )
         >>=
         wait_until_master
       end
    | _ -> Lwt.fail_with "cluster had problems to begin with"
  in
  test_with_alba_client
    (fun alba_client ->
     Lwt_log.debug_f "starting %s" test_name >>= fun () ->
     let units =
       let rec loop acc = function
         | 0 -> acc
         | i -> loop (()::acc) (i-1)
       in
       loop [] 20
     in
     let use_pool () =
       Lwt_list.map_p
         (alba_client # mgr_access # list_all_nsm_hosts)
         units
       >>= fun _ ->
       Lwt.return ()
     in

     use_pool () >>= fun () ->
     drop_master() >>= fun () ->

     Lwt_extra2.ignore_errors
       ~logging:true
       (fun () -> use_pool () )
     >>= fun ()->

     alba_client # mgr_access # list_all_nsm_hosts ()
     >>= fun (n, nsm_hosts) ->
     Lwt.return ()
    )

let test_update_policies () =
  let test_name = "test_update_policies" in
  test_with_alba_client
    (fun alba_client ->
     let namespace = test_name in
     let preset_name = test_name in
     let open Albamgr_protocol.Protocol in
     let preset =
       Preset.({ _DEFAULT with
                 policies = [ (2,1,2,3); ]; })
     in
     alba_client # mgr_access # create_preset preset_name preset >>= fun () ->
     alba_client # create_namespace ~namespace ~preset_name:(Some preset_name) ()
     >>= fun namespace_id ->
     _wait_for_osds alba_client namespace_id >>= fun () ->

     let assert_k_m mf k m =
       let open Nsm_model in
       let es, compression = match mf.Manifest.storage_scheme with
         | Storage_scheme.EncodeCompressEncrypt (es, c) -> es, c in
       let k', m', w = match es with
         | Encoding_scheme.RSVM (k, m, w) -> k, m, w in
       assert (k = k');
       assert (m = m')
     in

     let object_name = "1" in
     alba_client # get_base_client # upload_object_from_string
                 ~namespace
                 ~object_name
                 ~object_data:"a"
                 ~checksum_o:None
                 ~allow_overwrite:Nsm_model.NoPrevious >>= fun (mf1, _) ->

     assert_k_m mf1 2 1;

     alba_client # mgr_access # update_preset
                 preset_name
                 Preset.Update.({ policies' = Some [ (5,4,8,3); ]; }) >>= fun () ->

     alba_client # get_base_client # get_preset_cache # refresh ~preset_name >>= fun () ->

     alba_client # get_base_client # upload_object_from_string
                 ~namespace
                 ~object_name:"2"
                 ~object_data:"a"
                 ~checksum_o:None
                 ~allow_overwrite:Nsm_model.NoPrevious >>= fun (mf2, _) ->

     assert_k_m mf2 5 4;

     let maintenance_client = new Maintenance.client (alba_client # get_base_client) in
     maintenance_client # repair_by_policy_namespace ~namespace_id >>= fun () ->

     alba_client # get_object_manifest
                 ~consistent_read:true
                 ~should_cache:false
                 ~namespace
                 ~object_name >>= fun (_, id_mf_o) ->

     assert (id_mf_o <> None);
     let mf' = Option.get_some id_mf_o in
     assert_k_m mf' 5 4;

     Lwt.return ()
    )

let test_stale_manifest_download () =
  test_with_alba_client
    (fun alba_client ->
     let test_name = "test_stale_manifest_download" in
     let namespace = test_name in

     alba_client # create_namespace ~namespace ~preset_name:None ()
     >>= fun namespace_id ->

     let object_name = test_name in
     let object_length = 932 in
     let object_data = Bytes.create object_length in
     alba_client # get_base_client # upload_object_from_string
                 ~namespace
                 ~object_name
                 ~object_data
                 ~checksum_o:None
                 ~allow_overwrite:Nsm_model.NoPrevious
     >>= fun mf ->

     let download () =
       alba_client # download_object_to_string
                   ~namespace
                   ~object_name
                   ~consistent_read:false
                   ~should_cache:true
       >>= fun _ ->
       Lwt.return ()
     in
     let download_slices () =
       alba_client # download_object_slices
                   ~namespace
                   ~object_name
                   ~object_slices:[0L, object_length]
                   ~consistent_read:false
                   (fun _ _ _ _ -> Lwt.return ())
       >>= fun _ ->
       Lwt.return ()
     in
     let rewrite_obj () =
       Alba_client.with_client
         ~release_resources:true
         (ref (Albamgr_test.get_ccfg ()))
         (fun alba_client2 ->
          let maintenance_client =
            new Maintenance.client (alba_client2 # get_base_client) in
          alba_client2 # get_object_manifest'
                       ~namespace_id
                       ~object_name
                       ~consistent_read:true
                       ~should_cache:false
          >>= fun (_, manifest_o) ->
          let manifest = Option.get_some manifest_o in
          Repair.rewrite_object
            (alba_client2 # get_base_client)
            ~namespace_id
            ~manifest >>= fun () ->
          maintenance_client # clean_obsolete_keys_namespace
                             ~once:true ~namespace_id >>= fun () ->
          Lwt.return ())
     in

     download () >>= fun () ->
     rewrite_obj () >>= fun () ->
     download () >>= fun () ->
     rewrite_obj () >>= fun () ->
     download_slices () >>= fun () ->
     rewrite_obj () >>= fun () ->
     download_slices () >>= fun () ->
     Lwt.return ())

let test_object_sizes () =
  test_with_alba_client
    (fun client ->
     let test_name = "test_object_sizes" in
     let preset_name = test_name in
     let open Albamgr_protocol.Protocol in
     let preset' = Preset.({ _DEFAULT with
                             policies = [ (2,1,3,3); ];
                             fragment_size = 128; }) in
     client # mgr_access # create_preset preset_name preset' >>= fun () ->
     let namespace = test_name in
     client # create_namespace ~preset_name:(Some preset_name) ~namespace () >>= fun _ ->

     Lwt_list.iter_s
       (fun i ->
        let object_name = string_of_int i in
        let object_data = Bytes.create i in
        client # get_base_client # upload_object_from_string
               ~namespace
               ~object_name
               ~object_data
               ~checksum_o:None
               ~allow_overwrite:Nsm_model.NoPrevious
        >>= fun _ ->
        client # download_object_to_string
               ~namespace
               ~object_name
               ~consistent_read:false
               ~should_cache:true >>= fun object_data_o' ->
        assert (object_data = Option.get_some object_data_o');
        Lwt.return ())
       (Int.range 0 (20 + 128*2)))


let test_retry_download () =
  test_with_alba_client
    (fun client ->
     let test_name = "test_retry_download" in

     let open Albamgr_protocol.Protocol in
     let preset_name = test_name in
     let preset' = Preset.({ _DEFAULT with
                             policies = [ (2,1,3,3); ];
                             fragment_size = 128; }) in
     client # mgr_access # create_preset
            preset_name preset' >>= fun () ->

     let namespace = test_name in
     client # create_namespace
            ~preset_name:(Some preset_name)
            ~namespace () >>= fun namespace_id ->

     let object_name = test_name in
     let object_data =
       (* 2 chunks *)
       Lwt_bytes2.Lwt_bytes.create (2*2*128)
     in
     client # upload_object_from_bytes
            ~namespace
            ~object_name
            ~object_data
            ~checksum_o:None
            ~allow_overwrite:Nsm_model.NoPrevious
     >>= fun (mf, _) ->

     let bad_mf =
       let open Nsm_model.Manifest in
       let fragment_locations =
         [ List.hd_exn mf.fragment_locations;
           [ (Some 0l,0); (Some 0l,0); (Some 0l,0); ] ]
       in
       { mf with
         size = Int64.(add mf.size 5L);
         fragment_locations; }
     in

     let poison_mf_cache () =
       (* poison manifest cache so a retry will be needed
         to download the object *)
       Manifest_cache.ManifestCache.add
         (client # get_manifest_cache)
         namespace_id
         object_name
         bad_mf
     in
     let assert_stale res_o =
       (* assert a retry was needed due to a stale manifest *)
       let _, stats = Option.get_some res_o in
       assert
         (let open Alba_statistics.Statistics in
          snd stats.get_manifest_dh = Stale)
     in
     begin
       poison_mf_cache ();
       let output_file = "/tmp/" ^ test_name in
       client # download_object_to_file
              ~namespace
              ~object_name
              ~output_file
              ~consistent_read:false
              ~should_cache:false >>= fun res_o ->

       assert_stale res_o;
       Lwt_extra2.read_file output_file >>= fun object_data' ->
       assert (object_data' = Lwt_bytes.to_string object_data);
       Lwt.return ()
     end >>= fun () ->

     begin
       poison_mf_cache ();
       client # download_object_to_string
              ~namespace
              ~object_name
              ~consistent_read:false
              ~should_cache:false >>= function
       | None -> assert false
       | Some object_data' ->
          assert (object_data = Lwt_bytes.of_string object_data');
          Lwt.return ()
     end)

let test_list_objects_by_id () =
  let test_name = "test_list_objects_by_id" in
  let namespace = test_name in
  test_with_alba_client
    (fun alba_client ->
     alba_client # create_namespace
                 ~preset_name:None
                 ~namespace () >>= fun namespace_id ->

     let open Nsm_model in
     alba_client # get_base_client # upload_object_from_string
                 ~namespace
                 ~object_name:""
                 ~object_data:""
                 ~checksum_o:None
                 ~allow_overwrite:NoPrevious
     >>= fun (mf, _) ->

     let object_id = mf.Manifest.object_id in

     alba_client # with_nsm_client'
                 ~namespace_id
                 (fun client ->
                  client # list_objects_by_id
                         ~first:object_id ~finc:true
                         ~last:(Some (object_id, true))
                         ~max:100 ~reverse:false >>= fun ((cnt, objs), has_more) ->
                  assert (not has_more);
                  assert (cnt = 1);
                  assert (objs = [ mf; ]);
                  Lwt.return ()))

open OUnit

let suite = "alba_test" >:::[
    "test_delete_namespace" >:: test_delete_namespace;
    "test_upload_download" >:: test_upload_download;
    "test_clean_obsolete_keys" >:: test_clean_obsolete_keys;
    "test_garbage_collect" >:: test_garbage_collect;
    "test_create_namespaces" >:: test_create_namespaces;
    "test_partial_download" >:: test_partial_download;
    "test_encryption" >:: test_encryption;
    "test_discover_claimed" >:: test_discover_claimed;
    "test_change_osd_ip_port" >:: test_change_osd_ip_port;
    "test_repair_by_policy" >:: test_repair_by_policy;
    "test_disk_churn" >:: test_disk_churn;
    "test_missing_corrupted_fragment" >:: test_missing_corrupted_fragment;
    "test_full_asd" >:: test_full_asd;
    "test_versions" >:: test_versions;
    "test_replication" >:: test_replication;
    "test_add_disk" >:: test_add_disk;
    "test_invalidate_deleted_namespace" >:: test_invalidate_deleted_namespace;
    "test_master_switch" >:: test_master_switch;
    "test_stale_manifest_download" >:: test_stale_manifest_download;
    "test_update_policies" >:: test_update_policies;
    "test_object_sizes" >:: test_object_sizes;
    "test_retry_download" >:: test_retry_download;
    "test_list_objects_by_id" >:: test_list_objects_by_id;
  ]
