open Lwt.Infix

type peers = (string * int) list [@@ deriving show]
type src_type =
  | File of string
  | Etcd of peers * string
  [@@ deriving show]

let get_src cfg_uri =
  Lwt.catch
    (fun () ->
     let protocol,rest =
       Scanf.sscanf
         cfg_uri "%s@://%s"
         (fun s0 s1 -> s0,s1)
     in
     Lwt_io.eprintlf "protocol:%S rest:%S %!" protocol rest >>= fun () ->

     match protocol with
     | "etcd" ->
        let peers, path = Etcd.parse_url rest in
        Lwt.return (Etcd (peers,path))
     | "file" ->
        Lwt.return (File rest)
     | _ -> Lwt.fail_with "todo"
    )
    (fun exn ->
     Lwt_io.eprintlf "exn:%s assume 'file'" (Printexc.to_string exn)
     >>= fun () ->
     Lwt.return (File cfg_uri))
