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

open Object_reader
open Lwt.Infix

class object_reader
        (alba_client : Alba_base_client.client)
        namespace_id
        manifest =
  let size = Int64.to_int manifest.Nsm_model.Manifest.size in
  (object
  val mutable pos = 0

  method reset =
    pos <- 0;
    Lwt.return_unit

  method length =
    Lwt.return size

  method read cnt target =
    Alba_client_download_slices.download_object_slices_from_fresh_manifest
      (alba_client # mgr_access)
      (alba_client # nsm_host_access)
      (alba_client # get_preset_info)
      ~namespace_id
      ~manifest
      ~object_slices:[ (Int64.of_int pos, cnt, target, 0); ]
      ~fragment_statistics_cb:(fun _ -> ())
      (alba_client # osd_access)
      (alba_client # get_fragment_cache)
      ~cache_on_read:(alba_client # get_cache_on_read_write |> fst)
      (fun ~namespace_id ~object_name ~object_id ~chunk_id ~fragment_id ~location -> ())
      ~partial_osd_read:(alba_client # get_partial_osd_read)
      ~do_repair:false
      ~get_ns_preset_info:(alba_client # get_ns_preset_info)
      ~get_namespace_osds_info_cache:(alba_client # get_namespace_osds_info_cache)
      ~read_preference: (alba_client # read_preference)
    >>= fun _mfs ->
    pos <- pos + cnt;
    Lwt.return ()
end: reader)
