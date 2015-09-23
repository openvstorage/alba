(*
Copyright 2015 Open vStorage NV

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

open Fragment_cache
open Lwt.Infix

let run_with_fragment_cache size test test_name =
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



let test_1 () =
  let _inner (cache :Fragment_cache.blob_cache) =
    cache # clear_all () >>= fun () ->
    let blob = Bytes.create 4096 in
    let bid = 0l and oid = "0000" in
    cache # add bid oid blob  (* _add_new_grow *)
    >>= fun () ->

    Bytes.set blob 0 'x';
    cache # add bid oid blob  (* _replace_grow *)
    >>= fun () ->
    Bytes.set blob 0 'y';     (* _replace_grow *)
    cache # add bid oid blob
    >>= fun () ->
    Bytes.set blob 0 'z';     (* _replace_grow *)
    cache # add bid oid blob >>= fun () ->
    (* lookup *)
    cache # lookup bid oid >>=
      begin
        function
        | None ->
           OUnit.assert_bool "should have been found" false;
           Lwt.return ()
        | Some retrieved ->
           OUnit.assert_bool "first byte?" (retrieved.[0] = 'z');

           Lwt.return ()
      end >>= fun () ->
    OUnit.assert_bool "internal integrity check failed" (cache # _check ());
    Lwt.return ()
  in
  run_with_fragment_cache 10_000L _inner "test_1"

let test_2 () =
  let _inner (cache: Fragment_cache.blob_cache) =
    cache # clear_all () >>= fun () ->
    let blob = Bytes.create 4096 in
    cache # add 0l "X" blob >>= fun () -> (* _add_new_grow *)
    cache # add 0l "Y" blob >>= fun () -> (* _add_new_full *)
    cache # add 0l "Z" blob >>= fun () -> (* _add_new_full *)
    cache # add 0l "T" blob >>= fun () -> (* _add_new_full *)
    let printer = Int64.to_string in
    OUnit.assert_equal
      ~printer ~msg:"cache count" 1L (cache # get_count ()) ;
    OUnit.assert_equal
      ~printer ~msg:"cache total_size" 4096L (cache # get_total_size ());
    OUnit.assert_bool "internal integrity check" (cache # _check());
    Lwt.return ()
  in
  run_with_fragment_cache 5000L _inner "test_2"

let test_3 () =
  let blob_size = 4096 in
  let _inner (cache: Fragment_cache.blob_cache) =
    cache # clear_all () >>= fun () ->
    let blob = Bytes.create blob_size in
    cache # add 0l "X" blob >>= fun () -> (* _add_new_grow *)
    cache # add 0l "Y" blob >>= fun () -> (* _add_new_grow *)
    cache # add 0l "Z" blob >>= fun () -> (* _add_new_grow *)
    cache # add 0l "T" blob >>= fun () -> (* _add_new_grow *)
    cache # add 0l "A" blob >>= fun () -> (* _add_new_full *)
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
  run_with_fragment_cache size _inner "test_3"

let test_4 () =
  let blob_size = 4096 in
  let _inner (cache : Fragment_cache.blob_cache) =
    cache # clear_all() >>= fun () ->
    let blob = Bytes.create blob_size in
    let make_oid n = Printf.sprintf "%8i" n in
    let rec loop n =
      if n = 1000
      then Lwt.return ()
      else
        let oid = make_oid n in
        cache # add 0l oid blob >>= fun () ->
        begin
          if n > 100
          then
            let oid' = make_oid (n - 100) in
            cache # add 0l oid' blob
          else
            Lwt.return ()
        end
        >>= fun () ->
        loop (n+1)
    in
    loop 0 >>= fun () ->
    let _ = cache # _check () in
    Lwt.return ()
  in
  let size = 40_000_000L in
  run_with_fragment_cache size _inner "test_4"

let test_5 () =
  let blob_size = 4096 in
  let _inner (cache: Fragment_cache.blob_cache) =
    cache # clear_all () >>= fun () ->
    let blob = Bytes.create blob_size in
    let bid0 = 0l
    and bid1 = 1l
    in
    let rec fill = function
      | [] -> Lwt.return ()
      | oid :: oids ->
         cache # add bid0 oid blob >>= fun () ->
         cache # add bid1 oid blob >>= fun () ->
         fill oids
    in
    fill ["0000"; "0001";"0002";"0003"] >>= fun () ->
    let printer = Int64.to_string in
    OUnit.assert_equal
      ~printer ~msg:"cache count" 8L (cache # get_count ());
    cache # drop 0l >>= fun () ->

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
  run_with_fragment_cache 65536L _inner "test_5"


let test_long () =
  let _inner (cache :Fragment_cache.blob_cache) =
    cache #clear_all () >>= fun () ->
    let bid_to_drop1 = Int32.of_int 21 in
    cache # add bid_to_drop1 "oid" "blob" >>= fun () ->
    cache # drop bid_to_drop1 >>= fun () ->
    let bid_to_drop2 = Int32.of_int 20 in
    cache # add bid_to_drop2 "oid1" "blob" >>= fun () ->
    cache # add bid_to_drop2 "oid2" "blob" >>= fun () ->
    cache # drop bid_to_drop2 >>= fun () ->
    cache # drop 22l >>= fun () ->
    cache # drop 23l >>= fun () ->
    let make_blob () = Bytes.create (2048 + Random.int 2048) in
    let fill n =
      let rec loop i =
        if i = 0
        then Lwt.return ()
        else
          begin
            let bid = Random.int32 10l in
            let oid = Random.int 100  |> Printf.sprintf "%04x" in
            Lwt_io.printlf "i:%i" i >>= fun () ->
            let blob = make_blob() in
            cache # add bid oid blob >>= fun () ->
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
    cache # drop 0l >>= fun () ->
    assert (cache # _check ());
    Lwt.return ()
  in
  run_with_fragment_cache 20_000L _inner "test_long"

let suite =
let open OUnit in
"fragment_cache" >:::[
    "test_1" >:: test_1;
    "test_2" >:: test_2;
    "test_3" >:: test_3;
    "test_4" >:: test_4;
    "test_5" >:: test_5;
    "test_long" >:: test_long;
  ]
