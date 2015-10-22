open Prelude
open Proxy_client
open Lwt.Infix
open Generic_bench


let do_writes ~robust (client:proxy_client) progress n input_file period prefix _ namespace =
  let gen = make_key period prefix in
  let do_one i =
    let object_name = gen () in
    Lwt.catch
      (fun () ->
       client # write_object_fs
              ~namespace
              ~object_name
              ~input_file
              ~allow_overwrite:false ())
      (fun exn ->
       let rec inner delay =
         Lwt.catch
           (fun () ->
            client # write_object_fs
                   ~namespace
                   ~object_name
                   ~input_file
                   ~allow_overwrite:true () >>= fun () ->
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
      (client:proxy_client)
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
      (client:proxy_client)
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
      (client:proxy_client)
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

let do_scenarios
      host port
      n_clients n
      file_name power prefix slice_size namespace
      scenarios =
  let period = period_of_power power in
  Lwt_list.iter_s
    (fun scenario ->
     let progress = make_progress (n/100) in
     Lwt_list.iter_p
       (fun i ->
        Proxy_client.with_client
          host port
          (fun client ->
           scenario
             client
             progress
             (n/n_clients)
             file_name
             period
             (Printf.sprintf "%s_%i" prefix i)
             slice_size
             namespace
          ))
       (Int.range 0 n_clients))
    scenarios
