open Prelude
open Lwt.Infix
open Generic_bench


let do_writes ~robust client progress n input_files period prefix _ namespace =
  let gen = make_key period prefix in
  let no_files = List.length input_files in
  if no_files = 0
  then failwith "please specify files for 'writes' scenario";
  let do_one i =
    let object_name = gen () in
    let input_file = List.nth_exn input_files (i mod no_files) in
    let checksum : Checksum.Checksum.t option option = None in
    Lwt.catch
      (fun () ->
       client # write_object_fs
              ~namespace
              ~object_name
              ~input_file
              ~allow_overwrite:false
              ?checksum ())
      (fun exn ->
       let rec inner delay =
         Lwt.catch
           (fun () ->
            client # write_object_fs
                   ~namespace
                   ~object_name
                   ~input_file
                   ~allow_overwrite:true
                   ?checksum () >>= fun () ->
            Lwt.return `Continue)
           (fun exn ->
            Lwt.return `Retry) >>= function
         | `Continue ->
            Lwt.return ()
         | `Retry ->
            Lwt_unix.sleep delay >>= fun () ->
            inner (min 60. (delay *. 1.5))
       in
       if robust
       then inner 1.
       else Lwt.fail exn)
  in
  Lwt_io.printlf "writes (robust=%b):" robust >>= fun () ->
  measured_loop progress do_one n >>= fun r ->
  report "writes" r

let do_reads
      client
      progress n _ period prefix
      (_:int) namespace =
  let gen = make_key period prefix in
  let do_one i =
    let object_name = gen () in
    let output_file = Printf.sprintf "/tmp/%s_%s.tmp" namespace prefix in
    client # read_object_fs
           ~namespace
           ~object_name
           ~output_file
           ~consistent_read:false
           ~should_cache:true
  in
  Lwt_io.printlf "reads:" >>= fun () ->
  measured_loop progress do_one n >>= fun r ->
  report "reads" r

let do_partial_reads
      client
      progress n _ period prefix
      (slice_size:int) namespace =
  let gen = make_key period prefix in
  let do_one i =
    let object_name = gen () in
    let object_slices = [
        (object_name,[0L, slice_size]); (* TODO: what with smaller objects? *)
      ]
    in
    client # read_object_slices ~namespace ~object_slices ~consistent_read:false
    >>= fun _data ->
    Lwt.return ()
  in
  Lwt_io.printlf "partial reads:" >>= fun () ->
  measured_loop progress do_one n >>=fun r ->
  report "partial reads" r

let do_deletes
      client
      progress n _ period prefix
      (_:int) namespace =
  let gen = make_key period prefix in
  let do_one i =
    let object_name = gen ()  in
    client # delete_object ~namespace ~object_name ~may_not_exist:false
  in
  Lwt_io.printlf "deletes:" >>= fun () ->
  measured_loop progress do_one n >>= fun r ->
  report "deletes" r

let do_get_version
      client
      progress n
      _ _ _ _ _ =
  let do_one _ =
    client # get_version >>= fun _ ->
    Lwt.return_unit
  in
  Lwt_io.printlf "get_version:" >>= fun () ->
  measured_loop progress do_one n >>= fun r ->
  report "get_version" r


let do_scenarios
      host port transport
      n_clients n
      file_names power prefix slice_size namespace
      scenarios =
  let period = period_of_power power in
  Lwt_list.iter_s
    (fun scenario ->
     let progress = make_progress (n/100) in
     Lwt_list.iter_p
       (fun i ->
        Proxy_client.with_client
          host port transport
          (fun client ->
           scenario
             client
             progress
             (n/n_clients)
             file_names
             period
             (Printf.sprintf "%s_%i" prefix i)
             slice_size
             namespace
          ))
       (Int.range 0 n_clients))
    scenarios
