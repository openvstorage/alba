open Nsm_model

let deser =
    let _from_buffer buf =
      let inflater =
        match Llio2.ReadBuffer.int8_from buf with
        | 1 -> Manifest.inner_from_buffer_1
        | 2 -> Manifest.inner_from_buffer_2
        | k -> Prelude.raise_bad_tag "Nsm_model.Manifest" k
      in
      let s = Snappy.uncompress (Llio2.ReadBuffer.string_from buf) in
      Prelude.deserialize inflater s
    in
    let _to_buffer buf t =
      let res = Prelude.serialize Manifest.inner_to_buffer_1 t in
      let ser_version = 1 in
      Llio2.WriteBuffer.int8_to buf ser_version;
      Llio2.WriteBuffer.string_to buf (Snappy.compress res)
    in
    _from_buffer, _to_buffer
