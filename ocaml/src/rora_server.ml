external alba_start_rora_server :
  string ->
  string ->
  int ->
  int ->
  int ->
  int64 = "alba_start_rora_server"

let transport_s = function
  | Net_fd.TCP  -> "tcp"
  | Net_fd.RDMA -> "rdma"

let start transport host port cores queue_depth =
  alba_start_rora_server (transport_s transport) host port cores queue_depth

let _alternative_start =
  let open Foreign in
  let open Ctypes in
  foreign
    "alba_alternative_start"
    (string @-> string @-> int @-> int @-> int @-> returning int64_t)

let alternative_start transport host port cores queue_depth =
  _alternative_start (transport_s transport) host port cores queue_depth

external alba_stop_rora_server :
  int64 -> int = "alba_stop_rora_server"


let stop h =
  alba_stop_rora_server h
