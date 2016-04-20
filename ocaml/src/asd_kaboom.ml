open Cli_common
open Lwt.Infix
open Cmdliner
       
let buffer_pool = Buffer_pool.osd_buffer_pool
                    
let kaboom hosts port transport tls_config asd_id verbose =
  let conn_info = Networking2.make_conn_info hosts port ~transport tls_config in
  let one osd = 
    Alba_client_message_delivery._get_next_msg_id osd Osd.Low
    >>= fun (next_id_so,next_id) ->
    Lwt_io.printlf "next_id:%li" next_id
  in
  lwt_cmd_line
    ~to_json:false ~verbose
    (fun () ->
      Asd_client.with_client
        buffer_pool
        ~conn_info asd_id
        (fun client ->
          let osd1 = new Asd_client.asd_osd "xxx" client in
          Asd_client.with_client
            buffer_pool
            ~conn_info asd_id
            (fun client2 ->
              let osd2 = new Asd_client.asd_osd "xxx" client2 in
              one osd2 >>= fun () ->
              one osd1 >>= fun () ->
              one osd2 >>= fun () ->
              one osd1
            )
        )
    )
        (*
  run_with_asd_client'
    ~conn_info asd_id verbose
    (fun client ->
      let module DK = Osd_keys.AlbaInstance in
      let open Albamgr_protocol.Protocol.Osd.Message in
      let open Prelude in
      let next_id = 0l in
      let next_id_so = None
                         (* Some(Lwt_bytes.of_string (serialize Llio.int32_to next_id)) *)
      in
      let asserts, upds =
        let namespace_name = "demo"
        and namespace_id = 0l in
        let namespace_status_key = DK.namespace_status ~namespace_id in
        [ Osd.Assert.none_string namespace_status_key; ],
        [ Osd.Update.set_string
            namespace_status_key
            Osd.Osd_namespace_state.(serialize to_buffer Active)
            Checksum.NoChecksum true;
          Osd.Update.set_string
            (DK.namespace_name ~namespace_id) namespace_name
            Checksum.NoChecksum true;
        ]
      in
      let bump_msg_id =
        Osd.Update.set_string
          DK.next_msg_id
          (serialize Llio.int32_to (Int32.succ next_id))
          Checksum.NoChecksum
          true
      in
      let asserts' =
        Osd.Assert.value_option
          (Slice.wrap_string DK.next_msg_id)
          (Option.map
             (fun x -> Asd_protocol.Blob.Lwt_bytes x)
             next_id_so)
        :: asserts
      in
      client # apply_sequence
             Osd.Low
             asserts'
             (bump_msg_id :: upds)
    )
         *)

 let kaboom_cmd =
  let kaboom_t =
    Term.(pure kaboom
          $ hosts $ port 8000 $ transport $tls_config
          $ lido
          $ verbose
    )
  in
  let info =
    let doc = "kaboom" in
    Term.info "asd-kaboom" ~doc 
  in
  kaboom_t, info
