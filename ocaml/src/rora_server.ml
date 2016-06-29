open Foreign
open Ctypes

let transport_s = function
  | Net_fd.TCP  -> "tcp"
  | Net_fd.RDMA -> "rdma"

let _start =
  foreign
    "alba_start_rora_server"
    (string @-> string @-> int @-> int @-> int
     @-> string @-> int
     @-> int
     @-> returning int64_t)

let start transport host port cores queue_depth files_path files_path_length log_level =
  let log_level' =
    let open Lwt_log in
    match log_level with
    | Debug   -> 1
    | Info    -> 2
    | Notice  -> 2 (* boost doesn't have this *)
    | Warning -> 3
    | Error   -> 4
    | Fatal   -> 5
  in
  _start (transport_s transport) host port cores queue_depth
         files_path files_path_length log_level'


let stop h =
  let _function =
    foreign
      "alba_stop_rora_server"
      (int64_t @-> returning int)
  in
  _function h

let register_rocksdb (db:Rocks.RocksDb.t) =

  let ptr = Rocks.RocksDb.get_pointer db in
  let raw = Ctypes.raw_address_of_ptr ptr in
  let open Foreign in
  let open Ctypes in
  let _function =
    foreign
      "alba_register_rocksdb_c"
      (int64_t @-> returning void)
  in
  _function (Signed.Int64.of_nativeint raw)
