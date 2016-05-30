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
open Osd
open Lwt_bytes2
open Lwt.Infix

type kvs = Osd.key_value_storage

let no_checksum = Checksum.Checksum.NoChecksum

let apply (client : kvs) asserts updates =
  client # apply_sequence High asserts updates >>= function
  | Osd.Ok -> Lwt.return ()
  | Osd.Exn err -> Lwt.fail (Asd_protocol.Protocol.Error.Exn err)

let set_string (client : kvs) key value =
  apply client
        [] [ Osd.Update.set
               (Slice.wrap_string key)
               (Osd.Blob.Bytes value)
               no_checksum true; ]

let get_string (client : kvs) key =
  client # get_option High (Slice.wrap_string key)

let get_string_exn (client : kvs) key =
  client # get_exn High (Slice.wrap_string key)

let delete_string (client : kvs) key =
  apply client
        [] [ Osd.Update.delete (Slice.wrap_string key); ]

let range_all (client : kvs) ~max =
  list_all_x
    ~first:(Slice.wrap_string "")
    Std.id
    (client # range High ~last:None ~max ~reverse:false)

let test_set_get_delete ~verify_value (client : Osd.key_value_storage) =
  let key = "key" in
  let size = Asd_server.blob_threshold + 2 in
  let value = Bytes.make size 'a' in
  set_string client key value >>= fun () ->
  get_string_exn client key >>= fun v ->
  if verify_value
  then assert (let open Osd.Blob in
               equal (Lwt_bytes v) (Bytes value));
  delete_string client key

let test_multiget (client : Osd.key_value_storage) =
  let value1 = "fdidid" in
  let value2 = "vasi" in
  set_string client "key"  value1 >>= fun () ->
  set_string client "key2" value2 >>= fun () ->
  client # multi_get High
         (List.map
            Slice.wrap_string
            [ "key"; "key2"; ]) >>= fun res ->
  assert (2 = List.length res);
  assert ([ Some value1; Some value2; ] = List.map (Option.map Lwt_bytes.to_string) res);
  Lwt.return ()

let test_multi_exists client =
  let v = "xxxx" in
  let existing_key = "exists" in
  set_string client existing_key v >>= fun () ->
  client # multi_exists High [
           Slice.wrap_string existing_key;
           Slice.wrap_string "non_existing"
         ]
  >>= fun res ->
  Lwt_log.debug ([%show : bool list] res) >>= fun () ->
  assert (2 = List.length res);
  assert ([true;false] = res );
  Lwt.return ()

let test_range_query (client : kvs) =
  let v = "" in
  let set k = Osd.Update.set_string k v Checksum.Checksum.NoChecksum false in
  apply client
         []
         [ set "" ; set "k"; set "kg"; set "l"; ] >>= fun () ->

  client # range High
         ~first:(Slice.wrap_string "") ~finc:true ~last:None
         ~max:(-1) ~reverse:false >>= fun ((cnt, keys), _) ->
  let keys = List.map Slice.get_string_unsafe keys in

  Lwt_io.printlf
    "Found the following keys: %s"
    ([%show : string list] keys) >>= fun () ->
  assert (4 = cnt);
  assert (keys= [""; "k"; "kg"; "l";]);

  client # range High
         ~first:(Slice.wrap_string "l") ~finc:true
         ~last:(Some(Slice.wrap_string "o", false))
         ~max:(-1) ~reverse:false >>= fun ((cnt, keys), _) ->
  let keys = List.map Slice.get_string_unsafe keys in

  Lwt_io.printlf
    "Found the following keys: %s"
    ([%show : string list] keys) >>= fun () ->
  assert (1 = cnt);
  assert (keys = ["l";]);
  Lwt.return ()

let test_delete client =
  let key = "sda" in
  delete_string client key >>= fun () ->
  get_string client key >>= fun res ->
  assert (None = res);
  set_string client key key >>= fun () ->
  get_string client key >>= fun res ->
  assert (None <> res);
  delete_string client key >>= fun () ->
  get_string client key >>= fun res ->
  assert (None = res);
  Lwt.return ()

let test_list_all client =
  let rec add_keys = function
    | 100 -> Lwt.return ()
    | n ->
       set_string client (string_of_int n) "x" >>= fun () ->
       add_keys (n + 1)
  in
  add_keys 0 >>= fun () ->

  range_all client ~max:50 >>= fun (cnt, _) ->
  Lwt_log.debug_f "cnt = %i" cnt >>= fun () ->
  assert (cnt = 100);

  range_all client ~max:99 >>= fun (cnt, _) ->
  Lwt_log.debug_f "cnt = %i" cnt >>= fun () ->
  assert (cnt = 100);

  range_all client ~max:(-1) >>= fun (cnt, _) ->
  assert (cnt = 100);

  range_all client ~max:100 >>= fun (cnt, _) ->
  assert (cnt = 100);

  range_all client ~max:49 >>= fun (cnt, _) ->
  assert (cnt = 100);

  Lwt.return ()

let test_assert (client : kvs) =
  Lwt.catch
    (fun () ->
     apply client
         [ Osd.Assert.value_string "key" "value"; ]
         [ Osd.Update.set_string
             "key"
             (Bytes.create (Asd_server.blob_threshold + 2))
             Checksum.Checksum.NoChecksum false; ] >>= fun () ->
     assert false)
    (function
      | Asd_protocol.Protocol.Error.Exn Asd_protocol.Protocol.Error.Assert_failed _ ->
         Lwt.return_unit
      | exn ->
         Lwt_log.warning ~exn "Got the wrong exn!" >>= fun () ->
         Lwt.fail exn)
  >>= fun () ->
  apply client
      []
      [ Osd.Update.set_string
          "key"
          (Bytes.create (Asd_server.blob_threshold + 2))
          Checksum.Checksum.NoChecksum false; ] >>= fun () ->
  client # multi_get High [ Slice.wrap_string "x" ] >>= fun _ ->
  Lwt.return ()

let test_partial_get (client : kvs) =
  let key = Slice.wrap_string "key" in
  let inner size =
    let value = Lwt_bytes.create_random size in
    apply client
          [] [ Osd.Update.set key (Blob.Lwt_bytes value) no_checksum true; ] >>= fun () ->

    let inner' slices =
      let destination = Lwt_bytes.create size in
      let slices =
        List.map
          (fun (offset, length, destoff) ->
           offset, length, destination, destoff)
          slices
      in
      client # partial_get
          High
          key
          slices >>= fun success ->
      assert (success = Osd.Success);
      assert (value = destination);
      Lwt.return ()
    in

    inner' [ 0, size, 0 ] >>= fun () ->
    inner' [ 0, size - 15, 0;
             size - 15, 15, size - 15; ] >>= fun () ->
    inner' [ size - 15, 15, size - 15;
             0, size - 15, 0; ] >>= fun () ->
    Lwt.return ()
  in
  inner (Asd_server.blob_threshold - 5) >>= fun () ->
  inner (Asd_server.blob_threshold + 5)

let test_multi_update_for_same_key (client : kvs) =
  let key = "dfsjdi" in
  let value = Bytes.create (Asd_server.blob_threshold + 2) in
  let set = Osd.Update.set_string
              key value
              Checksum.Checksum.NoChecksum false
  in
  let delete = Osd.Update.delete_string key in
  apply client [] [ set; set; ] >>= fun () ->
  apply client [] [ set; set; set; set; set; ] >>= fun () ->
  apply client [] [ delete; delete; ] >>= fun () ->
  apply client [] [ delete; set; ] >>= fun () ->
  apply client [] [ set; delete; set; set; set; ] >>= fun () ->
  apply client [] [ delete; ] >>= fun () ->

  (* TODO verify no blobs are left on asd
   * size accounting should be 0
   * any other things that should be empty?
   *)

  Lwt.return ()
