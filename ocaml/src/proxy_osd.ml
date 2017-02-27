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

open Prelude
open Slice
open Lwt.Infix
open Alba_based_osd

class type proxy_pool =
  object
    method with_client : 'r.
                         namespace : string ->
                         (Proxy_client.proxy_client -> 'r Lwt.t) ->
                         'r Lwt.t
  end

class simple_proxy_pool ~ip ~port ~transport ~size =
  let factory () = Proxy_client.make_client ip port transport in
  let pool = Lwt_pool2.create
               size
               ~check:(fun _ exn -> false)
               ~factory
               ~cleanup:(fun (_, closer) -> closer ())
  in
  object(self :# proxy_pool)
    method with_client ~namespace f =
      Lwt_pool2.use pool (fun (client, _) -> f client)

    method finalize =
      Lwt_pool2.finalize pool
  end

class multi_proxy_pool
        ~(alba_id : string option)
        ~(endpoints : (Net_fd.transport * string * int) list ref)
        ~transport
        ~size =
  let fuse = ref false in
  let initial_resolve_t, initial_resolve_waiter = Lwt.wait () in

  let active_pools : (Net_fd.transport * string * int, simple_proxy_pool) Hashtbl.t = Hashtbl.create 3 in
  let disqualified_endpoints_requalify_threads = Hashtbl.create 3 in

  let affinity_mapping = Hashtbl.create 3 in

  let get_pool ~namespace =
    let get_new_pool () =
      let rec inner cnt =
        match Hashtbl.choose_random active_pools with
        | None ->
           if cnt = 1
           then Lwt.fail_with "no proxies available"
           else
             begin
               initial_resolve_t >>= fun () ->
               (Hashtbl.values disqualified_endpoints_requalify_threads
                |> List.map fst
                |> Lwt.choose) >>= fun () ->
               inner (cnt + 1)
             end
        | Some (endpoint, pp) ->
           pp # with_client ~namespace
              (fun client ->
                Lwt.catch
                  (fun () -> client # invalidate_cache ~namespace)
                  (let open Proxy_protocol.Protocol.Error in
                   function
                   | Exn (NamespaceDoesNotExist, _) -> Lwt.return ()
                   | exn -> Lwt.fail exn))
           >>= fun () ->
           Hashtbl.add affinity_mapping namespace endpoint;
           Lwt.return (endpoint, pp)
      in
      inner 0
    in
    match Hashtbl.find affinity_mapping namespace with
    | endpoint ->
       begin
         match Hashtbl.find active_pools endpoint with
         | pp -> Lwt.return (endpoint, pp)
         | exception Not_found -> get_new_pool ()
       end
    | exception Not_found -> get_new_pool ()
  in

  let requalify_endpoint ((transport, ip, port) as endpoint) =
    Hashtbl.remove active_pools endpoint;
    let fuse = ref false in
    let rec t () =
      let pp = new simple_proxy_pool ~ip ~port ~transport ~size in
      Lwt.catch
        (fun () ->
          pp # with_client
             ~namespace:""
             (fun client -> client # get_alba_id) >>= fun alba_id' ->
          match alba_id with
          | None -> Lwt.return `Done
          | Some alba_id ->
             if alba_id = alba_id'
             then Lwt.return `Done
             else
               begin
                 let msg = Printf.sprintf "proxy_osd expected alba_id %S but got %S instead"
                                          alba_id alba_id' in
                 Lwt_log.warning msg  >>= fun () ->
                 Lwt.fail_with msg
               end
        )
        (fun exn ->
          pp # finalize >>= fun () ->
          Lwt.return `TryAgain)
      >>= function
      | `Done ->
         Hashtbl.remove disqualified_endpoints_requalify_threads endpoint;
         Hashtbl.add active_pools endpoint pp;
         Lwt_log.debug_f "(re)qualified proxy %s://%s:%i" ([%show : Net_fd.transport] transport) ip port
      | `TryAgain ->
         if !fuse
         then
           begin
             Hashtbl.remove disqualified_endpoints_requalify_threads endpoint;
             Lwt.return ()
           end
         else
           begin
             Lwt_extra2.sleep_approx 60. >>= fun () ->
             t ()
           end
    in
    let t = t () in
    Hashtbl.add disqualified_endpoints_requalify_threads endpoint (t, fuse)
  in

  let () =
    let resolve_endpoints () =
      Lwt_list.map_p
        (fun (transport, host, port) ->
          Lwt.catch
            (fun () ->
              Lwt_unix.getaddrinfo host "" [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM; ] >>= fun r ->
              List.map
                (fun addr_info ->
                  transport,
                  (match addr_info.Unix.ai_addr with
                   | Unix.ADDR_UNIX x -> assert false
                   | Unix.ADDR_INET (x, _) -> Unix.string_of_inet_addr x),
                  port)
                r
              |> Lwt.return)
            (fun exn ->
              Lwt_log.info_f ~exn "Error while resolving host %S" host >>= fun () ->
              Lwt.return [])
        )
        !endpoints >>= fun resolveds ->
      Lwt.return (List.flatten_unordered resolveds)
    in
    let rec inner () =
      if !fuse
      then Lwt.return ()
      else
        begin
          resolve_endpoints () >>= fun resolved_endpoints ->

          Lwt_log.debug_f "proxy_osd: resolved endpoints to: %s"
                          ([%show : (Net_fd.transport * string * int) list] resolved_endpoints) >>= fun () ->

          let active_endpoints = Hashtbl.keys active_pools in
          let disqualified_endpoints = Hashtbl.keys disqualified_endpoints_requalify_threads in

          let all_current_endpoints = List.rev_append active_endpoints disqualified_endpoints in
          let new_endpoints =
            List.filter
              (fun endpoint -> not (List.mem endpoint all_current_endpoints))
              resolved_endpoints
          in

          List.iter requalify_endpoint new_endpoints;

          let active_to_remove =
            List.filter
              (fun endpoint -> not (List.mem endpoint resolved_endpoints))
              active_endpoints
          in
          List.iter
            (fun endpoint ->
              let pp = Hashtbl.find active_pools endpoint in
              pp # finalize |> Lwt.ignore_result;
              Hashtbl.remove active_pools endpoint)
            active_to_remove;

          let disqualified_to_remove =
            List.filter
              (fun endpoint -> not (List.mem endpoint resolved_endpoints))
              disqualified_endpoints
          in
          List.iter
            (fun endpoint -> (Hashtbl.find disqualified_endpoints_requalify_threads endpoint |> snd) := true)
            disqualified_to_remove;

          if Lwt.is_sleeping initial_resolve_t
          then Lwt.wakeup initial_resolve_waiter ();

          Lwt_extra2.sleep_approx 60. >>= fun () ->
          inner ()
        end
    in
    Lwt.ignore_result (inner ())
  in

  object(self :# proxy_pool)
    method with_client ~namespace f =
      if !fuse
      then Lwt.fail_with "multi_proxy_pool is being finalized"
      else
        get_pool ~namespace >>= fun (endpoint, pp) ->
        Lwt.catch
          (fun () -> pp # with_client ~namespace f)
          (fun exn ->
            requalify_endpoint endpoint;
            Lwt.fail exn)

    method finalize =
      Lwt_log.info_f "Finalizing multi_proxy_pool" >>= fun () ->
      fuse := true;
      Hashtbl.iter (fun _ p -> p # finalize |> Lwt.ignore_result) active_pools;
      Hashtbl.clear active_pools;
      Hashtbl.iter (fun _ (_, fuse) -> fuse := true) disqualified_endpoints_requalify_threads;
      Lwt.return ()
  end

let ensure_namespace_exists proxy_client ~namespace ~preset =
  proxy_client # get_namespace_preset ~namespace >>= function
  | None -> proxy_client # create_namespace ~namespace ~preset_name:(Some preset)
  | Some preset' ->
     if preset = preset'
     then Lwt.return ()
     else Lwt.fail_with (Printf.sprintf "ensure_namespace_exists %S %S: it already exist with wrong preset %S!"
                                        namespace preset preset')

class t
        (proxy_pool : proxy_pool)
        ~alba_id
        ~prefix ~preset
        ~namespace_name_format
  =
  let to_namespace_name = to_namespace_name prefix namespace_name_format in
  let get_kvs ~consistent_read namespace =
    object(self :# Osd.key_value_storage)
      method get_option _prio name =
        let object_name = Slice.get_string_unsafe name in
        proxy_pool
          # with_client
          ~namespace
          (fun proxy_client ->
            proxy_client # read_objects
                     ~namespace
                     ~object_names:[ object_name; ]
                     ~consistent_read
                     ~should_cache:(not consistent_read)
                     (fun (_, vs) ->
                       match vs with
                       | [ Some (mf, v); ] -> Lwt.return (Some (Bigstring_slice.extract_to_bigstring v))
                       | [ None; ] -> Lwt.return_none
                       | _ -> assert false))

      method get_exn prio name =
        self # get_option prio name >>= function
        | None -> Lwt.fail_with
                    (Printf.sprintf
                       "Could not get value for key %S"
                       (Slice.get_string_unsafe name))
        | Some v -> Lwt.return v

      method multi_get prio names =
        proxy_pool
          # with_client
          ~namespace
          (fun proxy_client ->
                   proxy_client # read_objects
                     ~namespace
                     ~object_names:(List.map
                                      Slice.get_string_unsafe
                                      names)
                     ~consistent_read
                     ~should_cache:(not consistent_read)
                     (fun (_, values) ->
                       List.map
                         (Option.map (fun (mf, v) -> Bigstring_slice.extract_to_bigstring v))
                         values
                       |> Lwt.return))

      method multi_exists _prio names =
        proxy_pool
          # with_client
          ~namespace
          (fun proxy_client ->
                   proxy_client # multi_exists
                     ~namespace
                     ~object_names:(List.map Slice.get_string_unsafe names))

      method range _prio ~first ~finc ~last ~reverse ~max =
        proxy_pool
          # with_client
          ~namespace
          (fun proxy_client ->
                   proxy_client # list_object
                     ~namespace
                     ~first:(Slice.get_string_unsafe first) ~finc
                     ~last:(Option.map
                              (fun (l, linc) -> Slice.get_string_unsafe l, linc)
                              last)
                     ~reverse ~max)
        >>= fun ((cnt, names), has_more) ->
        Lwt.return ((cnt, List.map Slice.wrap_string names), has_more)

      method range_entries prio ~first ~finc ~last ~reverse ~max =
        self # range
             prio
             ~first ~finc ~last
             ~reverse ~max
        >>= fun ((cnt, object_names), has_more) ->
        proxy_pool
          # with_client
          ~namespace
          (fun proxy_client ->
                   proxy_client # read_objects
                     ~namespace
                     ~object_names:(List.map Slice.get_string_unsafe object_names)
                     ~consistent_read
                     ~should_cache:(not consistent_read)
                     (fun (_, vos) ->
                       Lwt_list.map_s
                         (function
                          | None -> Lwt.fail_with "object missing in range entries"
                          | Some (mf, v) ->
                             Lwt.return (Bigstring_slice.extract_to_bigstring v,
                                         mf.Nsm_model.Manifest.checksum))
                         vos)) >>= fun res ->
        Lwt.return ((cnt,
                     List.map2
                       (fun object_name (v, cs) -> object_name, v, cs)
                       object_names
                       res),
                    has_more)

      method partial_get _prio name object_slices =
        Lwt.catch
          (fun () ->
            proxy_pool
              # with_client
              ~namespace
              (fun proxy_client ->
                       proxy_client # read_object_slices
                         ~namespace
                         ~object_slices:[ Slice.get_string_unsafe name,
                                          List.map
                                            (fun (offset, len, _, _) -> Int64.of_int offset, len)
                                            object_slices; ]
                         ~consistent_read) >>= fun data ->
            let () =
              List.fold_left
                (fun total_offset (_, len, dst, dstoff) ->
                  Lwt_bytes.blit_from_bytes
                    data
                    total_offset
                    dst
                    dstoff
                    len;
                  total_offset + len)
                0
                object_slices
              |> ignore
            in
            Lwt.return Osd.Success)
          (let open Proxy_protocol.Protocol.Error in
           function
             | Exn (ObjectDoesNotExist, _) -> Lwt.return Osd.NotFound
             | exn -> Lwt.fail exn)

      method apply_sequence prio asserts updates =
        Lwt.catch
          (fun () ->
            proxy_pool
              # with_client
              ~namespace
              (fun proxy_client ->
                proxy_client
                  # read_objects
                  ~namespace
                  ~object_names:(List.map
                                   (function
                                    | Osd.Assert.Value (key, _) -> Slice.get_string_unsafe key)
                                   asserts)
                  ~consistent_read:true
                  ~should_cache:false
                  (fun (_, values) ->
                    List.map2
                      (fun assert_ mf_v_o ->
                        let open Proxy_protocol.Protocol in
                        let failed_assert key = Osd.Error.(lwt_fail (Assert_failed (Slice.get_string_unsafe key))) in
                        match assert_, mf_v_o with
                        | Osd.Assert.Value (key, None), None ->
                           Lwt.return (Assert.ObjectDoesNotExist (Slice.get_string_unsafe key))
                        | Osd.Assert.Value (key, None), Some _
                        | Osd.Assert.Value (key, Some _), None ->
                           failed_assert key
                        | Osd.Assert.Value (key, Some v), Some (mf, v') ->
                           if Osd.Blob.(equal (Bigslice v') v)
                           then Lwt.return (Assert.ObjectHasId (Slice.get_string_unsafe key,
                                                                mf.Nsm_model.Manifest.object_id))
                           else failed_assert key
                      )
                      asserts values
                    |> Lwt_list.map_s Std.id
                  )
                >>= fun asserts' ->

                proxy_client # apply_sequence
                             ~write_barrier:false
                             ~namespace
                             ~asserts:asserts'
                             ~updates:(List.map
                                         (let open Proxy_protocol.Protocol in
                                          function
                                          | Osd.Update.Set (key, None) ->
                                             Update.DeleteObject (Slice.get_string_unsafe key)
                                          | Osd.Update.Set (key, Some (blob, cs, _)) ->
                                             Update.UploadObject (Slice.get_string_unsafe key,
                                                                  Osd.Blob.get_bigstring_slice blob,
                                                                  Some cs)
                                         )
                                         updates)
                >>= fun _ ->
                Lwt.return (Osd.Ok [])))
          (function
           | Proxy_protocol.Protocol.Error.Exn (Proxy_protocol.Protocol.Error.AssertFailed, key) ->
              Lwt.return Osd.(Exn (Error.Assert_failed (Option.get_some key)))
           | exn ->
              Lwt.fail exn)
    end
  in

  object(self :# Osd.osd)

    method global_kvs =
      get_kvs ~consistent_read:true prefix

    method namespace_kvs namespace_id =
      get_kvs ~consistent_read:false (to_namespace_name namespace_id)

    method add_namespace namespace_id =
      let namespace = to_namespace_name namespace_id in
      proxy_pool
        # with_client
        ~namespace
        (fun proxy_client -> ensure_namespace_exists proxy_client ~namespace ~preset)

    method delete_namespace namespace_id _ =
      let namespace = to_namespace_name namespace_id in
      proxy_pool
        # with_client
        ~namespace
        (fun proxy_client ->
          proxy_client # list_namespaces
                       ~first:namespace ~finc:true
                       ~last:(Some (namespace, true))
                       ~max:1 ~reverse:false
          >>= fun ((_, x), _) ->
          match x with
          | [] ->
             Lwt.return_none
          | [ _; ] ->
             proxy_client # delete_namespace ~namespace >>= fun () ->
             Lwt.return_none
          | _ -> assert false)

    method set_full _ = failwith "grmbl this method doesn't belong here."
    method set_slowness _ = failwith "set_slowness not implemented"

    method get_version = proxy_pool # with_client ~namespace:"" (fun c -> c # get_version)
    method get_long_id = alba_id
    method get_disk_usage = Lwt.return
                              (1000L, 2000L)
    (* (failwith "TODO return sth based on asd disk usage") *)
    method capabilities = Lwt.return (0, [])
  end
