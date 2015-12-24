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
