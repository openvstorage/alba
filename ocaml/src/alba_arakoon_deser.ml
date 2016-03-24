(*
Copyright 2016 iNuron NV

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

open Alba_arakoon

module Config =
  struct
    open Config
    let to_buffer buf (cluster_id, cfgs) =
      let module Llio = Llio2.WriteBuffer in
      let ser_version = 1 in
      Llio.int8_to buf ser_version;
      Llio.string_to buf cluster_id;
      Llio.hashtbl_to
        Llio.string_to
        (fun buf ncfg ->
         Llio.list_to Llio.string_to buf ncfg.ips;
         Llio.int_to buf ncfg.port)
        buf
        cfgs

    let from_buffer buf =
      let module Llio = Llio2.ReadBuffer in
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let cluster_id = Llio.string_from buf in
      let cfgs =
        Llio.hashtbl_from
          Llio.string_from
          (fun buf ->
           let ips = Llio.list_from Llio.string_from buf in
           let port = Llio.int_from buf in
           { ips; port; })
          buf in
      (cluster_id, cfgs)
  end
