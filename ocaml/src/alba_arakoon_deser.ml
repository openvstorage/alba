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
          (Llio.pair_from
             Llio.string_from
             (fun buf ->
              let ips = Llio.list_from Llio.string_from buf in
              let port = Llio.int_from buf in
              { ips; port; }))
          buf in
      (cluster_id, cfgs)
  end
