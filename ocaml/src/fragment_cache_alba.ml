(*
Copyright 2016 iNuron NV

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
open Lwt.Infix

type alba_fragment_cache_bucket_strategy =
  | OneOnOne of alba_fragment_cache_bucket_strategy_one_on_one [@name "1-to-1"]
[@@deriving yojson, show]

 and alba_fragment_cache_bucket_strategy_one_on_one = {
     preset : string;
     prefix : string;
   } [@@deriving yojson, show]

class alba_cache
        ~albamgr_cfg_ref
        ~bucket_strategy
        ~nested_fragment_cache
        ~manifest_cache_size
        ~albamgr_connection_pool_size
        ~nsm_host_connection_pool_size
        ~osd_connection_pool_size
        ~osd_timeout
        ~tls_config
  =
  let tcp_keepalive = Tcp_keepalive2.default in
  let albamgr_pool =
    Remotes.Pool.Albamgr.make
      ~size:albamgr_connection_pool_size
      albamgr_cfg_ref
      tls_config
      Buffer_pool.default_buffer_pool
      ~tcp_keepalive
  in
  let mgr_access =
    new Albamgr_client.client (new Albamgr_access.basic_mgr_pooled albamgr_pool)
  in
  let base_client = new Alba_base_client.client
                        nested_fragment_cache
                        ~mgr_access
                        ~manifest_cache_size
                        ~bad_fragment_callback:(fun alba_client
                                                    ~namespace_id
                                                    ~object_id ~object_name
                                                    ~chunk_id ~fragment_id
                                                    ~location -> ())
                        ~nsm_host_connection_pool_size
                        ~osd_connection_pool_size
                        ~osd_timeout
                        ~default_osd_priority:Osd.High
                        ~tls_config
                        ~tcp_keepalive
                        ~use_fadvise:true
  in
  let client = new Alba_client.alba_client base_client in
  let make_object_name ~bid ~name =
    serialize
      (Llio.pair_to
         Llio.int32_to
         Llio.string_to)
      (bid, name)
  in
  let with_namespace bid f =
    match bucket_strategy with
    | OneOnOne { prefix;
                 preset; } ->
       let namespace = prefix ^ (serialize ~buf_size:4 Llio.int32_to bid) in
       Lwt.catch
         (fun () -> f namespace)
         (fun exn ->
          client # create_namespace ~namespace ~preset_name:(Some preset) () >>= fun _ ->
          f namespace)
  in
  object(self :# Fragment_cache.cache)
    method add bid name blob =
      Lwt.catch
        (fun () ->
         with_namespace
           bid
           (fun namespace ->
            client # upload_object_from_bytes
                   ~namespace
                   ~object_name:(make_object_name ~bid ~name)
                   ~object_data:blob
                   ~checksum_o:None
                   ~allow_overwrite:Nsm_model.Unconditionally >>= fun _ ->
            Lwt.return_unit))
        (fun exn ->
         Lwt_log.debug_f ~exn "Exception while adding object to alba fragment cache")

    method lookup bid name =
      Lwt.catch
        (fun () ->
         with_namespace
           bid
           (fun namespace ->
            client # download_object_to_bytes
                   ~object_name:(make_object_name ~bid ~name)
                   ~namespace
                   ~consistent_read:false
                   ~should_cache:true))
        (fun exn ->
         Lwt_log.debug_f ~exn "Exception during alba fragment cache lookup" >>= fun () ->
         Lwt.return_none)

    method lookup2 bid name object_slices =
      Lwt.catch
        (fun () ->
         with_namespace
           bid
           (fun namespace ->
            client # nsm_host_access # with_namespace_id
                   ~namespace
                   (fun namespace_id ->
                    base_client # download_object_slices'
                                ~namespace_id
                                ~object_name:(make_object_name ~bid ~name)
                                ~object_slices:(List.map
                                                  (fun (offset, length, dest, dest_off) ->
                                                   Int64.of_int offset, length,
                                                   dest, dest_off)
                                                  object_slices)
                                ~consistent_read:false
                                ~fragment_statistics_cb:ignore
                   )) >>= function
         | None -> Lwt.return_false
         | Some _ -> Lwt.return_true)
        (fun exn ->
         Lwt_log.debug_f ~exn "Exception during alba fragment cache lookup2" >>= fun () ->
         Lwt.return_false)

    method drop bid =
      (* TODO maybe implement something here? *)
      Lwt.return_unit

    method close () =
      nested_fragment_cache # close () >>= fun () ->
      base_client # osd_access # finalize;
      base_client # nsm_host_access # finalize;
      Lwt_pool2.finalize albamgr_pool
  end
