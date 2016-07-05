open Nsm_model

let deser =
    let _from_buffer buf =
      match Llio2.ReadBuffer.int8_from buf with
      | 1 -> let s = Snappy.uncompress (Llio2.ReadBuffer.string_from buf) in
             Prelude.deserialize Manifest.from_buffer' s
      | k -> Prelude.raise_bad_tag "Nsm_model.Manifest" k
    in
    let _to_buffer buf t =
      let res = Prelude.serialize Manifest.to_buffer' t in
      let ser_version = 1 in
      Llio2.WriteBuffer.int8_to buf ser_version;
      Llio2.WriteBuffer.string_to buf (Snappy.compress res)
    in
    _from_buffer, _to_buffer
