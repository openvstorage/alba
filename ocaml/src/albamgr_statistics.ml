module Albamgr_statistics = struct
    let with_timing_lwt = Alba_statistics.Statistics.with_timing_lwt
    let with_timing = Alba_statistics.Statistics.with_timing
    open Stat
    include Stat

    type t = {
        mutable creation: Prelude.timestamp;
        mutable period : float;
        statistics : (int32, stat) Hashtbl.t;
      }

    let clone t = {t with creation = t.creation}
    let stop  t = t.period <- Unix.gettimeofday() -. t.creation
    let clear t =
      t.creation <- Unix.gettimeofday();
      t.period <- 0.0;
      Hashtbl.clear t.statistics


    (* TODO: maybe better to lift the names out of the protocol *)
    let _name_of = function
      | 3l -> "ListNsmHosts"
      | 4l -> "AddNsmHost"
      | 5l -> "UpdateNsmHost"
      | 6l -> "ListNamespaces"
      | 7l -> "CreateNamespace"
      | 8l -> "DeleteNamespace"
      |11l -> "ListNamespaceOsds"
      |15l -> "GetNextMsgs Msg_log.Nsm_host"
      |16l -> "MarkMsgDelivered Msg_log.Nsm_host"
      |17l -> "GetNextMsgs Msg_log.Osd"
      |18l -> "MarkMsgDelivered Msg_log.Osd"
      |19l -> "GetWork"
      |20l -> "MarkWorkCompleted"
      |22l -> "GetAlbaId"
      |23l -> "CreatePreset"
      |24l -> "ListPresets"
      |25l -> "SetDefaultPreset"
      |26l -> "DeletePreset"
      |27l -> "AddOsdsToPreset"
      |28l -> "AddOsd"
      |29l -> "MarkOsdClaimed"
      |30l -> "ListAvailableOsds"
      |31l -> "ListOsdsByOsdId"
      |32l -> "ListOsdsByLongId"
      |33l -> "MarkOsdClaimedByOther"
      |34l -> "UpdateOsd"
      |35l -> "AddWork"
      |36l -> "GetLicense"
      |37l -> "ApplyLicense"
      |38l -> "StoreClientConfig"
      |39l -> "GetClientConfig"
      |41l -> "ListNamespacesById"
      |42l -> "RecoverNamespace"
      |43l -> "GetActiveOsdCount"
      |44l -> "GetVersion"
      |45l -> "CheckClaimOsd"
      |46l -> "DecommissionOsd"
      |47l -> "ListDecommissioningOsds"
      |48l -> "ListOsdNamespaces"
      |49l -> "UpdateOsds"
      |50l -> "GetStatistics"
      | x  -> Printf.sprintf "??_stat_%li_??" x

    let show t =
      let b = Buffer.create 1024 in
      let add = Buffer.add_string b in
      add "{\n\tcreation: ";
      add ([%show: Prelude.timestamp] t.creation);
      add ";\n\tperiod: ";
      add (Printf.sprintf "%.2f" t.period);
      add ";\n\t ";
      Hashtbl.fold
        (fun k v acc ->
         add (_name_of k);
         add  " : ";
         add ([%show : stat] v);
         add ";\n\t")
        t.statistics ();
      add "}";
      Buffer.contents b

    let make () = {
        creation = Unix.gettimeofday();
        period  = 0.0;
        statistics = Hashtbl.create 50;
      }

    let _find_stat t tag =
        try Hashtbl.find t.statistics tag
        with Not_found -> Stat.make ()

    let new_query (t:t) tag delta =
      let stat = _find_stat t tag in
      let stat' = _update stat delta in
      Hashtbl.replace t.statistics tag stat'

    let new_update (t:t) tag delta =
      let stat = _find_stat t tag in
      let stat' = _update stat delta in
      Hashtbl.replace t.statistics tag stat'

    let to_buffer buf t =
      let ser_version = 1 in
      Llio.int8_to buf ser_version;
      Llio.float_to buf t.creation;
      Llio.float_to buf t.period;
      Llio.hashtbl_to Llio.int32_to stat_to buf t.statistics

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let creation = Llio.float_from buf in
      let period   = Llio.float_from buf in
      let statistics =
        let ef buf = Llio.pair_from Llio.int32_from stat_from buf in
        Llio.hashtbl_from ef buf
      in
      { creation; period; statistics}

  end
