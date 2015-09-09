open Cli_common
open Lwt.Infix
open Cmdliner

let alba_list_osd_messages cfg_file attempts (destinations:int32 list) =
  let t () =
    with_albamgr_client
      cfg_file ~attempts
      (fun client ->
       let open Albamgr_protocol in
       Lwt_list.fold_left_s
         (fun (c0,xs0) destination ->
          client # get_next_msgs Protocol.Msg_log.Osd destination
          >>= fun ((c,xs),_more) ->
          let xs' = List.map (fun (msg_id,msg) -> (destination,msg_id, msg)) xs in
          Lwt.return (c + c0, xs' @ xs0)
         ) (0,[]) (List.rev destinations)
       >>= fun (c,xs) ->
       Lwt_io.printlf "destination | msg_id | message" >>= fun () ->
       Lwt_io.printlf "------------+--------+--------" >>= fun () ->
       Lwt_list.iter_s
         (fun (destination, msg_id,msg) ->
          Lwt_io.printlf
            "%11li |%7li | %s"
            destination msg_id ([%show: Protocol.Osd.Message.t] msg)>>= fun () ->
          Lwt.return ()
         ) xs

      )
  in
  lwt_cmd_line false t

let alba_list_osd_messages_cmd =
  let destinations  =
    let doc = "destinations" in
    Arg.(value
         & opt (list ~sep:',' int32) []
         & info ["destinations"] ~docv:"DESTINATION" ~doc
    )
  in
  Term.(pure alba_list_osd_messages $ alba_cfg_file $ attempts 1 $ destinations),
  Term.(info "list-osd-messages" ~doc:"list messages from mgr to osds")

let cmds =[
    alba_list_osd_messages_cmd
  ]
