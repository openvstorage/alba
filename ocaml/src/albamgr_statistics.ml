open Prelude

module Albamgr_statistics = struct
    let with_timing_lwt = Alba_statistics.Statistics.with_timing_lwt
    let with_timing = Alba_statistics.Statistics.with_timing
    open Stat
    include Stat

    type t = {
        mutable creation: timestamp;

        mutable list_nsm_hosts     : stat;
        mutable list_namespaces    : stat;
        mutable create_namespace   : stat;
        mutable delete_namespace   : stat;
        mutable list_namespace_osds: stat;
        mutable get_next_msgs      : stat;
        mutable mark_msg_delivered : stat;
        mutable mark_work_completed: stat;
        mutable create_preset      : stat;
        mutable set_default_preset : stat;
        mutable delete_preset      : stat;
        mutable add_osd_to_preset  : stat;

        mutable update_osds        : stat;
      } [@@deriving show]

    let make () = {
        creation = Unix.gettimeofday();
        list_nsm_hosts      = Stat.make ();
        list_namespaces     = Stat.make ();
        create_namespace    = Stat.make ();
        delete_namespace    = Stat.make ();
        list_namespace_osds = Stat.make ();
        get_next_msgs       = Stat.make ();
        mark_msg_delivered  = Stat.make ();
        mark_work_completed = Stat.make ();
        create_preset       = Stat.make ();
        set_default_preset  = Stat.make ();
        delete_preset       = Stat.make ();
        add_osd_to_preset   = Stat.make ();

        update_osds         = Stat.make ()
      }

    let new_query (t:t) tag delta =
      Plugin_helper.debug_f "new_query:%li %f" tag delta;
      match tag with
      |  3l -> t.list_nsm_hosts      <- _update t.list_nsm_hosts      delta
      |  6l -> t.list_namespaces     <- _update t.list_namespaces     delta
      | 11l -> t.list_namespace_osds <- _update t.list_namespace_osds delta
      | 15l -> t.get_next_msgs       <- _update t.get_next_msgs       delta
      |_ -> ()

    let new_update (t:t) tag delta =
      match tag with
      |  7l -> t.create_namespace    <- _update t.create_namespace     delta
      |  8l -> t.delete_namespace    <- _update t.delete_namespace     delta
      | 16l -> t.mark_msg_delivered  <- _update t.mark_msg_delivered   delta
      | 20l -> t.mark_work_completed <- _update t.mark_work_completed  delta
      | 23l -> t.create_preset       <- _update t.create_preset        delta
      | 25l -> t.set_default_preset  <- _update t.set_default_preset   delta
      | 26l -> t.delete_preset       <- _update t.delete_preset        delta
      | 27l -> t.add_osd_to_preset   <- _update t.add_osd_to_preset    delta
      | 49l -> t.update_osds         <- _update t.update_osds          delta
      | _   -> ()

    let to_buffer buf t =
      let ser_version = 1 in
      Llio.int8_to buf ser_version;
      Llio.float_to buf t.creation;
      List.iter (fun x -> stat_to buf x)
                [ t.list_nsm_hosts;
                  t.list_namespaces;
                  t.create_namespace;
                  t.delete_namespace;
                  t.list_namespace_osds;
                  t.get_next_msgs;
                  t.mark_msg_delivered;
                  t.mark_work_completed;
                  t.create_preset;
                  t.set_default_preset;
                  t.delete_preset;
                  t.add_osd_to_preset;
                  t.update_osds;
                ]

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let creation = Llio.float_from buf in
      let list_nsm_hosts     = stat_from buf in
      let list_namespaces    = stat_from buf in
      let create_namespace   = stat_from buf in
      let delete_namespace   = stat_from buf in
      let list_namespace_osds= stat_from buf in
      let get_next_msgs      = stat_from buf in
      let mark_msg_delivered = stat_from buf in
      let mark_work_completed= stat_from buf in
      let create_preset      = stat_from buf in
      let set_default_preset = stat_from buf in
      let delete_preset      = stat_from buf in
      let add_osd_to_preset  = stat_from buf in
      let update_osds        = stat_from buf in
      { creation;
        list_nsm_hosts;
        list_namespaces;
        create_namespace;
        delete_namespace;
        list_namespace_osds;
        get_next_msgs;
        mark_msg_delivered;
        mark_work_completed;
        create_preset;
        set_default_preset;
        delete_preset;
        add_osd_to_preset;
        update_osds;
      }
  end
