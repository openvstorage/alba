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
open Lwt.Infix
open Generic_bench

class alba_bench_client alba_client =
object
  method write_object_fs
        ~namespace ~object_name
        ~input_file
        ~allow_overwrite
        ?(checksum = None) () =
    alba_client # upload_object_from_file
                ~namespace
                ~object_name ~input_file
                ~checksum_o:checksum
                ~allow_overwrite:(let open Nsm_model in
                                  if allow_overwrite
                                  then Unconditionally
                                  else NoPrevious)
    >>= fun _ ->
    Lwt.return ()

    method read_object_fs
      ~namespace ~object_name
      ~output_file
      ~consistent_read
      ~should_cache
      =
      alba_client # download_object_to_file
                  ~namespace
                  ~object_name
                  ~output_file
                  ~consistent_read ~should_cache
      >>= function
      | None -> let open Proxy_protocol.Protocol in
                Error.(failwith ObjectDoesNotExist)
      | Some _ -> Lwt.return ()

    method read_object_slices ~namespace ~object_slices ~consistent_read =
      Proxy_server.read_objects_slices
        alba_client
        namespace object_slices ~consistent_read

    method delete_object ~namespace ~object_name ~may_not_exist=
      alba_client # delete_object ~namespace ~object_name ~may_not_exist

end

let do_scenarios
      albamgr_cfg
      n_clients n
      file_name power prefix slice_size namespace
      scenarios =
  let period = period_of_power power in
  Alba_client.with_client
    ~tcp_keepalive:Tcp_keepalive2.default
    albamgr_cfg
    (fun alba_client ->
     let client = new alba_bench_client alba_client in
     Lwt_list.iter_s
       (fun scenario ->
        let progress = make_progress (n/100) in
        Lwt_list.iter_p
          (fun i ->
           scenario
             client
             progress
             (n/n_clients)
             file_name
             period
             (Printf.sprintf "%s_%i" prefix i)
             slice_size
             namespace
          )
          (Int.range 0 n_clients))
       scenarios)
