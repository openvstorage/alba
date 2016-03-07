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
    alba_client # download_object_slices''
                ~namespace_id
                ~manifest
                ~object_slices:[ (Int64.of_int pos, cnt, target, 0); ]
                ~fragment_statistics_cb:(fun _ -> ())
    >>= fun _ ->
    pos <- pos + cnt;
    Lwt.return ()
end: reader)
