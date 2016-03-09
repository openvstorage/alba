open Prelude

module OsdInfo =
  struct
    open Nsm_model.OsdInfo

    let to_buffer buf t =
      let json = to_yojson t in
      let s = Yojson.Safe.to_string json in
      Llio2.WriteBuffer.string_to buf s
    let from_buffer buf =
      let s = Llio2.ReadBuffer.string_from buf in
      let json = Yojson.Safe.from_string s in
      match of_yojson json with
      | `Error s -> failwith s
      | `Ok t -> t
  end

module ClaimInfo =
  struct
    open Albamgr_protocol.Protocol.Osd.ClaimInfo

    let to_buffer buf =
      let module Llio = Llio2.WriteBuffer in
      function
      | ThisAlba osd_id ->
         Llio.int8_to buf 1;
         Llio.int32_to buf osd_id
      | AnotherAlba alba ->
         Llio.int8_to buf 2;
         Llio.string_to buf alba
      | Available ->
         Llio.int8_to buf 3

    let from_buffer buf =
      let module Llio = Llio2.ReadBuffer in
      match Llio.int8_from buf with
      | 1 -> ThisAlba (Llio.int32_from buf)
      | 2 -> AnotherAlba (Llio.string_from buf)
      | 3 -> Available
      | k -> raise_bad_tag "Osd.ClaimInfo" k

    let deser = from_buffer, to_buffer
  end