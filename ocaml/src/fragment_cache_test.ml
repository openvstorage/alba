(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Fragment_cache
open Lwt.Infix

let run_with_local_fragment_cache size test test_name =
  Random.init 666;
  let dir = Printf.sprintf "/tmp/alba/blob_cache_tests/%s" test_name
  in
  let cmd0 = Printf.sprintf "rm -rf %s" dir in
  let cmd1 = Printf.sprintf "mkdir -p %s" dir in
  let _rc = Sys.command cmd0 in
  let _rc = Sys.command cmd1 in

  let t =
    Lwt.catch
      (fun () ->
       Fragment_cache_fs.safe_create dir ~max_size:size ~rocksdb_max_open_files:256
       >>= fun cache ->
       test cache >>= fun () ->
       OUnit.assert_bool "internal integrity check failed" (cache # _check ());
       Lwt.catch
         (fun () -> cache # close ())
         (fun exn -> Lwt_log.warning_f ~exn "closing failed:%s" test_name)
      )
      (fun exn ->
       Lwt_log.warning_f ~exn "%s failed" test_name
       >>= fun () ->
       Lwt.fail exn
      )
  in
  let () = Test_extra.lwt_run t in
  ()

let run_with_alba_fragment_cache test test_name =
  let t () =
    Alba_test._fetch_abm_client_cfg () >>= fun abm_ccfg ->
    let tls_config = Albamgr_test.get_tls_config () in
    let cache =
      let open Fragment_cache_alba in
      new alba_cache
          ~albamgr_cfg_ref:(ref abm_ccfg)
          ~bucket_strategy:(OneOnOne { preset = "default";
                                       prefix = test_name; })
          ~nested_fragment_cache:(new no_cache)
          ~manifest_cache_size:100
          ~albamgr_connection_pool_size:10
          ~nsm_host_connection_pool_size:10
          ~osd_connection_pool_size:10
          ~osd_timeout:10.
          ~tls_config
          ~partial_osd_read:false
          ~cache_on_read:true ~cache_on_write:true
          ~albamgr_refresh_config:`None
    in
    test cache
  in
  Test_extra.lwt_run (t ())

let test_1 () =
  let _inner (cache :Fragment_cache_fs.blob_cache) =
    let blob = Bigstring_slice.create_random 4096 in
    let bid = 0L and oid = "0000" in
    cache # add' bid oid blob  (* _add_new_grow *)
    >>= fun () ->

    Bigstring_slice.set blob 0 'x';
    cache # add' bid oid blob  (* _replace_grow *)
    >>= fun () ->
    Bigstring_slice.set blob 0 'y';     (* _replace_grow *)
    cache # add' bid oid blob
    >>= fun () ->
    Bigstring_slice.set blob 0 'z';     (* _replace_grow *)
    cache # add' bid oid blob >>= fun () ->
    (* lookup *)
    cache # lookup ~timeout:5.0 bid oid >>=
      begin
        function
        | None ->
           OUnit.assert_bool "should have been found" false;
           Lwt.return ()
        | Some (retrieved, _) ->
           let bytes = SharedBuffer.deref retrieved in
           OUnit.assert_equal ~msg:"first byte?" 'z' (Lwt_bytes.get bytes 0);
           let () = SharedBuffer.unregister_usage retrieved in
           Lwt.return_unit
      end >>= fun () ->
    Lwt.return ()
  in
  run_with_local_fragment_cache 10_000L _inner "test_1"

let test_2 () =
  let _inner (cache: Fragment_cache_fs.blob_cache) =
    let blob = Bigstring_slice.create_random 4096 in
    cache # add' 0L "X" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0L "Y" blob >>= fun () -> (* _add_new_full *)
    cache # add' 0L "Z" blob >>= fun () -> (* _add_new_full *)
    cache # add' 0L "T" blob >>= fun () -> (* _add_new_full *)
    let printer = Int64.to_string in
    OUnit.assert_equal
      ~printer ~msg:"cache count" 1L (cache # get_count ()) ;
    OUnit.assert_equal
      ~printer ~msg:"cache total_size" 4096L (cache # get_total_size ());
    OUnit.assert_bool "internal integrity check" (cache # _check());
    Lwt.return ()
  in
  run_with_local_fragment_cache 5000L _inner "test_2"

let test_3 () =
  let blob_size = 4096 in
  let _inner (cache: Fragment_cache_fs.blob_cache) =
    let blob = Bigstring_slice.create_random blob_size in
    cache # add' 0L "X" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0L "Y" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0L "Z" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0L "T" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0L "A" blob >>= fun () -> (* _add_new_full *)
    let printer = Int64.to_string in
    OUnit.assert_equal
      ~printer ~msg:"cache count" 4L (cache # get_count ());
    let rec loop =
      function
      | [] -> Lwt.return ()
      | (k, is_some)::rest ->
         begin
           cache # lookup ~timeout:5.0 0L k >>= fun r ->
           match r, is_some with
           | None  , false  -> Lwt.return_unit
           | Some (retrieved,_), true   ->
              let () = SharedBuffer.unregister_usage retrieved in
              Lwt.return_unit
           | _,_ -> OUnit.assert_bool k false; Lwt.return_unit
         end >>= fun () ->
         loop rest
    in
    loop ["T", true;
          "Z", true;
          "Y", true;
          "X", false;
         ]
    >>= fun () ->
    OUnit.assert_bool "internal integrity check" (cache # _check ());

    Lwt.return ()
  in
  let size = Int64.of_int (4 * blob_size + 1) in
  run_with_local_fragment_cache size _inner "test_3"

let test_4 () =
  let blob_size = 4096 in
  let _inner (cache : Fragment_cache_fs.blob_cache) =
    let blob = Bigstring_slice.create_random blob_size in
    let make_oid n = Printf.sprintf "%8i" n in
    let rec loop n =
      if n = 1000
      then Lwt.return_unit
      else
        let oid = make_oid n in
        cache # add' 0L oid blob >>= fun () ->
        begin
          if n > 100
          then
            let oid' = make_oid (n - 100) in
            cache # add' 0L oid' blob
          else
            Lwt.return_unit
        end
        >>= fun () ->
        loop (n+1)
    in
    loop 0
  in
  let size = 40_000_000L in
  run_with_local_fragment_cache size _inner "test_4"

let test_5 () =
  let blob_size = 4096 in
  let _inner (cache: Fragment_cache_fs.blob_cache) =
    let blob = Bigstring_slice.create_random blob_size in
    let bid0 = 0L
    and bid1 = 1L
    in
    let rec fill = function
      | [] -> Lwt.return ()
      | oid :: oids ->
         cache # add' bid0 oid blob >>= fun () ->
         cache # add' bid1 oid blob >>= fun () ->
         fill oids
    in
    fill ["0000"; "0001";"0002";"0003"] >>= fun () ->
    let printer = Int64.to_string in
    OUnit.assert_equal
      ~printer ~msg:"cache count" 8L (cache # get_count ());
    cache # drop ~global:true 0L >>= fun () ->

    (* evict the first 4 victims (should all come from bid 0) *)
    let rec inner = function
      | 4 -> Lwt.return ()
      | n ->
        cache # _evict
          ~total_size:(cache # get_total_size ())
          ~total_count:(cache # get_count ())
          ~victims_size:1 >>= fun _ ->
        inner (n+1)
    in
    inner 0 >>= fun () ->

    OUnit.assert_equal
      ~printer ~msg:"total_size" 16384L (cache # get_total_size ());
    OUnit.assert_equal
      ~printer ~msg:"cache count after" 4L (cache # get_count());
    OUnit.assert_bool "_check failed" (cache # _check ());
    Lwt.return ()
  in
  run_with_local_fragment_cache 65536L _inner "test_5"


let test_long () =
  let _inner (cache :Fragment_cache_fs.blob_cache) =
    let bid_to_drop1 = 21L in
    let blob = Bigstring_slice.of_string "blob" in
    cache # add' bid_to_drop1 "oid" blob >>= fun () ->
    cache # drop ~global:true bid_to_drop1 >>= fun () ->
    let bid_to_drop2 = 20L in
    cache # add' bid_to_drop2 "oid1" blob >>= fun () ->
    cache # add' bid_to_drop2 "oid2" blob >>= fun () ->
    cache # drop ~global:true bid_to_drop2 >>= fun () ->
    cache # drop ~global:true 22L >>= fun () ->
    cache # drop ~global:true 23L >>= fun () ->
    let make_blob () = Bigstring_slice.create_random (2048 + Random.int 2048) in
    let fill n =
      let rec loop i =
        if i = 0
        then Lwt.return_unit
        else
          begin
            let bid = Random.int64 10L in
            let oid = Random.int 100  |> Printf.sprintf "%04x" in
            Lwt_io.printlf "i:%i" i >>= fun () ->
            let blob = make_blob() in
            cache # add' bid oid blob >>= fun () ->
            let count = cache # get_count () in
            Lwt_log.debug_f "after add in fill, count=%Li" count >>= fun () ->
            loop (i-1)
          end
      in
      loop n
    in
    let fetch n =
      let rec loop found missed i =
        assert (cache # _check ());
        if i = 0
        then Lwt.return (found,missed)
        else
          begin
            let bid = Random.int64 10L in
            let oid = Random.int 100 |> Printf.sprintf "%04x" in
            cache # lookup ~timeout:5.0 bid oid
            >>= function
            | None -> loop (found    ) (missed + 1) (i-1)
            | Some (r,_)  ->
               let () = SharedBuffer.unregister_usage r in
               loop (found + 1)     missed   (i-1)

          end
      in
      loop 0 0 n
    in
    let t0 = Unix.gettimeofday () in
    fill 1000 >>= fun () ->
    let t1 = Unix.gettimeofday () in
    let d = t1 -. t0 in
    Lwt_log.debug_f "fill loop took: %3f" d >>= fun () ->
    fetch 1000 >>= fun (found, missed) ->
    Lwt_io.printlf "done: found:%i missed:%i" found missed >>= fun () ->
    cache # drop ~global:true 0L >>= fun () ->
    Lwt.return ()
  in
  run_with_local_fragment_cache 20_000L _inner "test_long"

let test_remove_local_cache () =
  let test_name = "test_remove_local_cache" in
  let max_size = 65536L in
  let root = Printf.sprintf "/tmp/alba/blob_cache_tests/%s" test_name in
  let t () =
    let _ = Sys.command (Printf.sprintf "mkdir -p %s" root) in
    Fragment_cache_fs.safe_create root ~max_size ~rocksdb_max_open_files:256
    >>= fun cache ->
    let bid = 0L in
    let value = Bigstring_slice.of_string "value" in
    cache # add' bid "key" value >>= fun () ->
    cache # add' bid "key2" value >>= fun () ->
    let some_file = Printf.sprintf "%s/jfsdlafsdjk" root in
    let _ = Sys.command (Printf.sprintf "touch %s" some_file) in
    let _ = Sys.command (Printf.sprintf "find %s" root) in
    Lwt_extra2.exists some_file >>= fun r ->
    assert r;

    cache # close' ~write_marker:false () >>= fun () ->

    Fragment_cache_fs.safe_create root ~max_size ~rocksdb_max_open_files:256
    >>= fun cache ->

    cache # close () >>= fun () ->

    let _ = Sys.command (Printf.sprintf "find %s" root) in
    Lwt_extra2.exists some_file >>= fun r ->
    assert (not r);

    Lwt.return ()
  in
  Test_extra.lwt_run (t ())

let _test_lookup2 (cache : cache) =
  let bid = 0L in
  let key = "key" in
  let size = 1_000_000 in
  let value = Lwt_bytes.create_random size in
  cache # add' bid key (Bigstring_slice.wrap_bigstring value) >>= fun () ->

  let inner slices =
    let destination = Lwt_bytes.create size in
    let slices =
      List.map
        (fun (offset, length, destoff) ->
         offset, length, destination, destoff)
        slices
    in
    cache # lookup2 ~timeout:5.0 bid key slices >>= fun (success, _) ->
    assert success;
    assert (value = destination);
    Lwt.return_unit
  in

  inner [ 0, size, 0 ] >>= fun () ->
  inner [ 0, size - 15, 0;
          size - 15, 15, size - 15; ] >>= fun () ->
  inner [ size - 15, 15, size - 15;
          0, size - 15, 0; ] >>= fun () ->

  (* test regular lookup too *)
  cache # lookup ~timeout:5.0 bid key >>= function
  | None -> assert false
  | Some (sr, _) ->
     let r = SharedBuffer.deref sr in
     assert (value = r);
     let () = SharedBuffer.unregister_usage sr in
     Lwt.return_unit

let test_lookup2_local () =
  run_with_local_fragment_cache
    1_000_000L
    (fun c -> _test_lookup2 (c :> cache))
    "test_lookup2"

let test_lookup2_alba () =
  run_with_alba_fragment_cache
    (fun c -> _test_lookup2 (c :> cache))
    "test_lookup2"

let test_x_cache () =
  run_with_local_fragment_cache
    1_000_000L
    (fun c ->
      let () = c # set_delay_after_add (Some 0.5) in
      let () = c # set_delay_after_lookup (Some 0.5) in
      let x = new Fragment_cache.x_cache (c:> cache) in
      let n = 20 in
      let item_range = Int.range 1 (n+1) in
      let shared = Lwt_bytes.create_random 100_000 |> SharedBuffer.make_shared in
      let blob = Bigstring_slice.wrap_shared_buffer shared in
      let test_concurrent_add () =
        Lwt_list.map_p
          (fun i -> x # add 0L "the_item" blob) item_range
        >>= fun _ -> Lwt.return_unit
      in

      let test_concurrent_lookup () =
        Lwt_list.map_p
          (fun i -> x # lookup ~timeout:1.0 0L "the_item" ) item_range
        >>= fun results ->
        let first,_ = List.hd_exn results |> Option.get_some in
        let degree = first.SharedBuffer.ref_count in
        Lwt_log.debug_f "share_degree:%i" degree
        >>= fun () ->
        OUnit.assert_equal ~msg:"degree after lookup" n degree (* ? *);
        List.iter (fun r ->
            let buf,_ = Option.get_some r in
            SharedBuffer.unregister_usage buf
          ) results ;
        let degree' = first.SharedBuffer.ref_count in
        Lwt_log.debug_f "share_degree':%i" degree' >>= fun () ->
        OUnit.assert_equal ~msg:"degree at end" 0 degree';
        Lwt.return_unit
      in
      let test_concurrent_lookup2 () =
        Lwt_log.debug "test_concurrent_lookup2" >>= fun () ->
        let key = "concurrent_lookup2" in
        let lookups () =
          Lwt_list.iter_p
            (fun i ->
              let slices = [   0, 100,Lwt_bytes.create 100, 0;
                               5000, 100,Lwt_bytes.create 100, 0;
                           ]
              in
              x # lookup2 ~timeout:1.0 0L key slices >>= fun (success, _) ->
              OUnit.assert_bool "lookup should be successful" success;
              List.iter
                (fun (off, len, dest, dest_off) ->
                  let rec cmp_bytes i =
                    if i = len
                    then ()
                    else
                      let b0 = Bigstring_slice.get blob (off + i) in
                      let s0 = Lwt_bytes.get dest (dest_off + i) in
                      let () = OUnit.assert_equal b0 s0 in
                      cmp_bytes (i+1)
                  in
                  cmp_bytes 0
                ) slices;
              Lwt.return_unit
            ) item_range
        in
        let add () =  x # add 0L key blob >>= fun _ -> Lwt.return_unit in
        Lwt.join
          [ add () ;
            Lwt_unix.sleep 0.001 >>= fun () -> lookups ();
          ]
        >>= fun () ->
        Lwt.return_unit
      in
      test_concurrent_add () >>= fun () ->
      test_concurrent_lookup () >>= fun () ->
      test_concurrent_lookup2 () >>= fun () ->
      let () = SharedBuffer.unregister_usage shared in
      Lwt.return_unit
    )
    "test_x_cache"

let suite =
let open OUnit in
"fragment_cache" >:::[
    "test_1" >:: test_1;
    "test_2" >:: test_2;
    "test_3" >:: test_3;
    "test_4" >:: test_4;
    "test_5" >:: test_5;
    "test_remove_local_cache" >:: test_remove_local_cache;
    "test_long" >:: test_long;
    "test_lookup2_local" >:: test_lookup2_local;
    "test_lookup2_alba" >:: test_lookup2_alba;
    "test_x_cache" >:: test_x_cache;
  ]
