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

open Fragment_cache
open Lwt.Infix
open Lwt_bytes2

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
       safe_create dir ~max_size:size ~rocksdb_max_open_files:256
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
  let () = Lwt_main.run t in
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
    in
    test cache
  in
  Lwt_main.run (t ())

let test_1 () =
  let _inner (cache :Fragment_cache.blob_cache) =
    let blob = Bigstring_slice.create_random 4096 in
    let bid = 0l and oid = "0000" in
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
    cache # lookup bid oid >>=
      begin
        function
        | None ->
           OUnit.assert_bool "should have been found" false;
           Lwt.return ()
        | Some (retrieved, _) ->
           OUnit.assert_bool "first byte?" (Lwt_bytes.get retrieved 0 = 'z');

           Lwt.return ()
      end >>= fun () ->
    Lwt.return ()
  in
  run_with_local_fragment_cache 10_000L _inner "test_1"

let test_2 () =
  let _inner (cache: Fragment_cache.blob_cache) =
    let blob = Bigstring_slice.create_random 4096 in
    cache # add' 0l "X" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0l "Y" blob >>= fun () -> (* _add_new_full *)
    cache # add' 0l "Z" blob >>= fun () -> (* _add_new_full *)
    cache # add' 0l "T" blob >>= fun () -> (* _add_new_full *)
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
  let _inner (cache: Fragment_cache.blob_cache) =
    let blob = Bigstring_slice.create_random blob_size in
    cache # add' 0l "X" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0l "Y" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0l "Z" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0l "T" blob >>= fun () -> (* _add_new_grow *)
    cache # add' 0l "A" blob >>= fun () -> (* _add_new_full *)
    let printer = Int64.to_string in
    OUnit.assert_equal
      ~printer ~msg:"cache count" 4L (cache # get_count ());
    let rec loop =
      function
      | [] -> Lwt.return ()
      | (k, is_some)::rest ->
         begin
           cache # lookup 0l k >>= fun r ->
           match r, is_some with
           | None  , false  -> Lwt.return ()
           | Some _, true   -> Lwt.return ()
           | _,_ -> OUnit.assert_bool k false; Lwt.return ()
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
  let _inner (cache : Fragment_cache.blob_cache) =
    let blob = Bigstring_slice.create_random blob_size in
    let make_oid n = Printf.sprintf "%8i" n in
    let rec loop n =
      if n = 1000
      then Lwt.return_unit
      else
        let oid = make_oid n in
        cache # add' 0l oid blob >>= fun () ->
        begin
          if n > 100
          then
            let oid' = make_oid (n - 100) in
            cache # add' 0l oid' blob
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
  let _inner (cache: Fragment_cache.blob_cache) =
    let blob = Bigstring_slice.create_random blob_size in
    let bid0 = 0l
    and bid1 = 1l
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
    cache # drop ~global:true 0l >>= fun () ->

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
  let _inner (cache :Fragment_cache.blob_cache) =
    let bid_to_drop1 = Int32.of_int 21 in
    let blob = Bigstring_slice.of_string "blob" in
    cache # add' bid_to_drop1 "oid" blob >>= fun () ->
    cache # drop ~global:true bid_to_drop1 >>= fun () ->
    let bid_to_drop2 = Int32.of_int 20 in
    cache # add' bid_to_drop2 "oid1" blob >>= fun () ->
    cache # add' bid_to_drop2 "oid2" blob >>= fun () ->
    cache # drop ~global:true bid_to_drop2 >>= fun () ->
    cache # drop ~global:true 22l >>= fun () ->
    cache # drop ~global:true 23l >>= fun () ->
    let make_blob () = Bigstring_slice.create_random (2048 + Random.int 2048) in
    let fill n =
      let rec loop i =
        if i = 0
        then Lwt.return_unit
        else
          begin
            let bid = Random.int32 10l in
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
            let bid = Random.int32 10l in
            let oid = Random.int 100 |> Printf.sprintf "%04x" in
            cache # lookup bid oid
            >>= function
            | None ->    loop (found    ) (missed + 1) (i-1)
            | Some _  -> loop (found + 1)     missed   (i-1)

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
    cache # drop ~global:true 0l >>= fun () ->
    Lwt.return ()
  in
  run_with_local_fragment_cache 20_000L _inner "test_long"

let test_remove_local_cache () =
  let test_name = "test_remove_local_cache" in
  let max_size = 65536L in
  let root = Printf.sprintf "/tmp/alba/blob_cache_tests/%s" test_name in
  let t () =
    let _ = Sys.command (Printf.sprintf "mkdir -p %s" root) in
    safe_create root ~max_size ~rocksdb_max_open_files:256
    >>= fun cache ->
    let bid = 0l in
    let value = Bigstring_slice.of_string "value" in
    cache # add' bid "key" value >>= fun () ->
    cache # add' bid "key2" value >>= fun () ->
    let some_file = Printf.sprintf "%s/jfsdlafsdjk" root in
    let _ = Sys.command (Printf.sprintf "touch %s" some_file) in
    let _ = Sys.command (Printf.sprintf "find %s" root) in
    Lwt_extra2.exists some_file >>= fun r ->
    assert r;

    cache # close' ~write_marker:false () >>= fun () ->

    safe_create root ~max_size ~rocksdb_max_open_files:256
    >>= fun cache ->

    cache # close () >>= fun () ->

    let _ = Sys.command (Printf.sprintf "find %s" root) in
    Lwt_extra2.exists some_file >>= fun r ->
    assert (not r);

    Lwt.return ()
  in
  Lwt_main.run (t ())

let _test_lookup2 (cache : cache) =
  let bid = 0l in
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
    cache # lookup2 bid key slices >>= fun (success, _) ->
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
  cache # lookup bid key >>= function
  | None -> assert false
  | Some (r, _) ->
     assert (value = r);
     Lwt.return ()

let test_lookup2_local () =
  run_with_local_fragment_cache
    1_000_000L
    (fun c -> _test_lookup2 (c :> cache))
    "test_lookup2"

let test_lookup2_alba () =
  run_with_alba_fragment_cache
    (fun c -> _test_lookup2 (c :> cache))
    "test_lookup2"

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
  ]
