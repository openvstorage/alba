(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

let default_enable_tcp_keepalive = true
let default_tcp_keepalive_time = 20
let default_tcp_keepalive_intvl = 20
let default_tcp_keepalive_probes = 3

type t = Tcp_keepalive.t = {
      enable_tcp_keepalive : (bool [@default default_enable_tcp_keepalive]);
      tcp_keepalive_time : (int [@default default_tcp_keepalive_time]);
      tcp_keepalive_intvl : (int [@default default_tcp_keepalive_intvl]);
      tcp_keepalive_probes : (int [@default default_tcp_keepalive_probes]);
    } [@@deriving yojson, show]

let default = {
    enable_tcp_keepalive = default_enable_tcp_keepalive;
    tcp_keepalive_time = default_tcp_keepalive_time;
    tcp_keepalive_intvl = default_tcp_keepalive_intvl;
    tcp_keepalive_probes = default_tcp_keepalive_probes;
  }
