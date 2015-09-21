open Cli_common
open Cmdliner
open Lwt.Infix

let nsm_host_statistics cfg_file clear nsm_host =
  let t () =
    with_alba_client
      cfg_file
      (fun client ->
       client # nsm_host_access # statistics nsm_host clear >>= fun statistics ->
       Lwt_io.printlf "%s" (Nsm_host_protocol.Protocol.NSMHStatistics.show statistics)
      )
  in
  lwt_cmd_line false t

let nsm_host_statistics_cmd =
  Term.(pure nsm_host_statistics
        $ alba_cfg_file
        $ clear
        $ nsm_host 0
  ),
  Term.info
    "nsm-host-statistics"
    ~doc:"namespace hosts' statistics"

let cmds =
  [
    nsm_host_statistics_cmd
  ]
