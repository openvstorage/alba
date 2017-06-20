(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
open Alba_arakoon

module Config =
  struct
    open Config

    let node_cfg_to_buffer buf { ips; port; } =
      let module Llio = Llio2.WriteBuffer in
      Llio.list_to Llio.string_to buf ips;
      Llio.int_to buf port

    let node_cfg_from_buffer buf =
      let module Llio = Llio2.ReadBuffer in
      let ips = Llio.list_from Llio.string_from buf in
      let port = Llio.int_from buf in
      { ips; port; }

    let to_buffer buf { cluster_id;
                        node_cfgs;
                        ssl_cfg;
                        tcp_keepalive;
                      } =
      let module Llio = Llio2.WriteBuffer in
      if ssl_cfg = None
         && tcp_keepalive = Tcp_keepalive.default_tcp_keepalive
      then
        begin
          let ser_version = 1 in
          Llio.int8_to buf ser_version;
          Llio.string_to buf cluster_id;
          Llio.list_to
            (Llio.pair_to
               Llio.string_to
               node_cfg_to_buffer)
            buf
            node_cfgs
        end
      else
        begin
          let _ : Llio.t =
            Llio.serialize_with_length'
            (fun buf () ->
              let ser_version = 2 in
              Llio.int8_to buf ser_version;
              Llio.string_to buf cluster_id;

              Llio.list_to
                (Llio.pair_to
                   Llio.string_to
                   node_cfg_to_buffer)
                buf
                node_cfgs;

              let { enable_tcp_keepalive;
                    tcp_keepalive_time;
                    tcp_keepalive_intvl;
                    tcp_keepalive_probes; } = tcp_keepalive in
              Llio.bool_to buf enable_tcp_keepalive;
              Llio.int_to buf tcp_keepalive_time;
              Llio.int_to buf tcp_keepalive_intvl;
              Llio.int_to buf tcp_keepalive_probes;

              Llio.option_to
                (fun buf { ca_cert; creds; protocol; } ->
                  Llio.string_to buf ca_cert;
                  Llio.option_to
                    (Llio.pair_to Llio.string_to Llio.string_to)
                    buf creds;
                  Llio.int8_to
                    buf
                    (match protocol with
                     | SSLv23  -> 1
                     | SSLv3   -> 2
                     | TLSv1   -> 3
                     | TLSv1_1 -> 4
                     | TLSv1_2 -> 5)
                )
                buf
                ssl_cfg)
            ~buf
            ()
          in
          ()
        end

    let from_buffer buf =
      let module Llio = Llio2.ReadBuffer in
      let version = Llio.int8_from buf in
      match version with
      | 1 ->
         let cluster_id = Llio.string_from buf in
         let node_cfgs =
           Llio.list_from
             (Llio.pair_from
                Llio.string_from
                node_cfg_from_buffer)
             buf in
         { cluster_id;
           node_cfgs;
           ssl_cfg = None;
           tcp_keepalive = Tcp_keepalive.default_tcp_keepalive; }
      | 2 ->
         Llio.deserialize_length_prefixed
           (fun buf ->
             let cluster_id = Llio.string_from buf in

             let node_cfgs =
               Llio.list_from
                 (Llio.pair_from
                    Llio.string_from
                    node_cfg_from_buffer)
                 buf in

             let enable_tcp_keepalive = Llio.bool_from buf in
             let tcp_keepalive_time = Llio.int_from buf in
             let tcp_keepalive_intvl = Llio.int_from buf in
             let tcp_keepalive_probes = Llio.int_from buf in
             let tcp_keepalive = {
                 enable_tcp_keepalive;
                 tcp_keepalive_time;
                 tcp_keepalive_intvl;
                 tcp_keepalive_probes;
               }
             in

             let ssl_cfg =
               Llio.option_from
                 (fun buf ->
                   let ca_cert = Llio.string_from buf in
                   let creds =
                     Llio.option_from
                       (Llio.pair_from Llio.string_from Llio.string_from)
                       buf
                   in
                   let protocol =
                     match Llio.int8_from buf with
                     | 1 -> SSLv23
                     | 2 -> SSLv3
                     | 3 -> TLSv1
                     | 4 -> TLSv1_1
                     | 5 -> TLSv1_2
                     | k -> raise_bad_tag "Alba_arakoon.Config.ssl_cfg.protocol" k
                   in
                   { ca_cert; creds; protocol; })
                 buf
             in

             { cluster_id;
               node_cfgs;
               ssl_cfg;
               tcp_keepalive; })
           buf
      | k ->
         raise_bad_tag "Alba_arakoon.Config.t" k
  end
