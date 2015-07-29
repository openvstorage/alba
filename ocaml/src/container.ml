open Lwt.Infix
open Prelude

module Container = struct
  type t = {
    path : string;
    fd : Lwt_unix.file_descr;
    check_entry : string -> bool;
  }

  type entry = {
    sequence_nr : int;
    blob : string;
  }

  let make path =
    Lwt_unix.openfile 
      path
      Lwt_unix.([ O_RDWR; O_CREAT; ])
      0o664 >>= fun fd ->

    (* TODO expected size hint meegeven aan filesystem *)

    let t = { path;
              fd; } in

    Gc.finalise
      (fun t -> Lwt.ignore_result (Lwt_unix.close t.fd))
      t;
    Lwt.return t

  (* checksum error in laatste block: OK, wss geen data loss
     checksum error in ander block: fuck, data loss

     probleem is dat je geen onderscheid kunt maken

     hmm, mss met O_DIRECT dat je ordering garanties krijgt?
 *)

  (* hmm nee, read entry is niet nodig... *)
  let read_entry t offset =
    let size_bs = Bytes.create 4 in
    Lwt_extra2.read_all t.fd bs 0 4 >>= fun cnt ->
    if cnt <> 4
    then Lwt.return (`EndOfFile (offset, cnt))
    else begin
      let e_length = deserialize Llio.int32_from bs in
      let entry_bytes = Bytes.create e_length in
      Lwt_extra2.read_all
        t.fd
        entry_bytes
        e_length >>= fun got ->
      if got < e_length
      then Lwt.return (`EndOfFile (offset, cnt + got))
      else begin
        
        Lwt.return `fdsa
      end
    end


  (* 
container moet concept van een page kennen
meh nee, in eerste instantie niet.
kan het parametriseren over raw fd of buffered io (functor)

NEE NEE NEE

itereren over container is niet de common case, het is uitzonderlijk!
enkel bij startup! dus nobody cares hoe efficient het al dan nie is.

 *)


  (* 
- length
- sequence number
- checksum om te verifieren dat entry volledig op disk staat?
  maar dan moet ik alle data checksummen en dat wil ik niet?
  wel ... mss wil client dat wel, maar dan moeten ze het zelf doen
  in laag hier nog es bovenop.
  deze laag doet enkel checksum op basis van
   - vorige entry + length + sequence number + current offset in file
 *)

  (* start iterating at a known valid offset *)
  (* let iter_entries t offset = *)

  (* iter entries om (extern) te indexeren wat er in deze container zit *)
  (* raw lezen van fd descriptor als je offset + size al kent *)

  (* let write t slice = *)
    
end
