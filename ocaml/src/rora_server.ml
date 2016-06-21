external alba_start_rora_server :
  string ->
  string ->
  int ->
  int ->
  int ->
  int64 = "alba_start_rora_server"

let start transport host port number_cores queue_depth =
  let transport_s = match transport with
    | Net_fd.TCP -> "tcp"
    | Net_fd.RDMA -> "rdma"
  in
  alba_start_rora_server transport_s host port number_cores queue_depth

external alba_stop_rora_server :
  int64 -> int = "alba_stop_rora_server"


let stop h =
  alba_stop_rora_server h
