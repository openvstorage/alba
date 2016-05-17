(*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*)

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
