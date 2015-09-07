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

open Lwt
open Slice
open Kinetic_client
open Osd
open Checksum

let test_sequence1 client =
  Lwt_io.printlf "----- test_sequence1 -----" >>= fun () ->
  let key = "zzz" in
  let key_s = Slice.wrap_string key in
  let value = key in
  let value_s = Slice.wrap_string value in
  let make_crc s =
    let algo = Checksum.Algo.CRC32c in
    let hasher = Hashes.make_hash algo in
    let () = hasher # update_string s in
    hasher # final ()
  in
  let check_v = make_crc value

  in

  (* clean up, delete *)
  let ass0 = []
  and upd0 = [Update.Set(key_s,None)] in
  client # apply_sequence ass0 upd0 >>= fun _ ->

  (* only update if No value was there *)
  let ass1 = [Assert.Value(key_s, None)]
  and upd1 = [Update.Set(key_s, Some (value_s, check_v, true))]
  in
  client # apply_sequence ass1 upd1 >>= fun _ ->

  (* only opdate if value matches *)
  let value2 = "There, I said it" in
  let value2_s = Slice.wrap_string value2 in
  let check_v2 = make_crc value2 in
  let ass2 = [Assert.Value(key_s, Some value_s);]
  and upd2 = [Update.Set(key_s, Some(value2_s, check_v2, true))]
  in
  client # apply_sequence ass2 upd2 >>= fun _ ->
  client # get_exn key_s >>= fun v3 ->
  assert (value2 = Slice.get_string_unsafe v3);
  Lwt.catch
    (fun () ->
      let ass3 = [Assert.Value(key_s, Some value_s);]
      and upd3 = [Update.Set(key_s, None);]
      in
      Lwt_log.debug_f "==> going to apply the bad sequence <==" >>= fun () ->
      client # apply_sequence ass3 upd3 >>= fun _ ->
      Lwt_log.debug_f "xxx> applied it, no problems ? <xxx" >>= fun () ->
      Lwt.return false
    )
    (function
      | Kinetic.Kinetic.Kinetic_exc _ -> Lwt.return true
      | _ -> Lwt.fail_with "wrong Exception"
    )
  >>= (function
  | true -> Lwt.return ()
  | false -> Lwt.fail_with "should not get here"
      )
  >>= fun () ->
  Lwt_log.info "+++ all's well +++" >>= fun () ->
  Lwt.return ()

let test_bug (client:Kinetic_client.kinetic_client) =
  let key_s = Slice.wrap_string "The Key" in
  let value_s = Slice.wrap_string "" in
  let upd = [Osd.Update.Set(key_s, Some (value_s,Checksum.NoChecksum,true));] in

  client # apply_sequence [] upd >>= fun _ ->
  client # get_option key_s >>= fun vo ->
  let dump vo =
    begin
      match vo with
      | None -> Lwt_io.printlf "it's None"
      | Some v -> Lwt_io.printlf "it's %S" (Slice.get_string_unsafe v)
    end
  in
  dump vo >>= fun () ->
  let key2 = Slice.wrap_string "Other_key" in
  client # get_option key2 >>= fun vo ->
  dump vo >>= fun () ->
  Lwt.return ()

let test_scenario (client:Kinetic_client.kinetic_client) =
  let key = "xxx" in
  let key_s = Slice.wrap_string key in
  client # get_option key_s >>= fun vo_s ->
  Lwt_io.printlf "----- get -----" >>= fun () ->
  Lwt_io.printlf "d[%S]=%s"
                 key
                 ([%show : Slice.t option] vo_s) >>= fun () ->

  Lwt_io.printlf "----- range -----" >>= fun () ->
  let first = Slice.wrap_string "a" in
  let finc = true in
  let last = None in
  let reverse = false in
  let max = 10 in
  client # range ~first ~finc ~last ~reverse ~max >>= fun ((n,keys), has_more) ->
  Lwt_list.iter_s
    (fun k ->
     Lwt_io.printlf "%S" (Slice.get_string_unsafe k)
    ) keys


let () =
  let () = Cli_common.install_logger() in
  let t =
    Kinetic_client.make_client ["::1"] 11000 "blabla"
    >>= fun (client, closer) ->
    Lwt.finalize
      (fun () ->
       test_scenario client >>= fun () ->
       test_sequence1 client >>= fun () ->
       test_bug client >>= fun () ->
       Lwt.return ()
      )
      (fun () -> closer ())
  in
  Lwt_main.run t
