(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

type redis_lru_cache_eviction = {
    host : string;
    port : int;
    key : string;
   } [@@deriving yojson, show]

let redis_lru_cache_eviction_from_buffer buf =
  let host = Llio.string_from buf in
  let port = Llio.int_from buf in
  let key = Llio.string_from buf in
  { host; port; key; }

let redis_lru_cache_eviction_to_buffer buf { host; port; key; } =
  Llio.string_to buf host;
  Llio.int_to buf port;
  Llio.string_to buf key

type t = {
    enable_auto_repair : bool;
    auto_repair_timeout_seconds : float;
    auto_repair_disabled_nodes : string list;

    enable_rebalance : bool;

    cache_eviction_prefix_preset_pairs : (string, string) Hashtbl.t;
    redis_lru_cache_eviction : redis_lru_cache_eviction option;
  } [@@deriving yojson]

let show t = to_yojson t |> Yojson.Safe.pretty_to_string

let get_prefixes t =
  Hashtbl.fold
    (fun prefix _ acc -> prefix :: acc)
    t.cache_eviction_prefix_preset_pairs
    []

let from_buffer buf =
  let ser_version = Llio.int8_from buf in
  assert (ser_version = 1);
  let enable_auto_repair = Llio.bool_from buf in
  let auto_repair_timeout_seconds = Llio.float_from buf in
  let auto_repair_disabled_nodes = Llio.list_from Llio.string_from buf in
  let enable_rebalance = Llio.bool_from buf in
  let cache_eviction_prefix_preset_pairs =
    maybe_from_buffer
      (Llio.hashtbl_from
         (Llio.pair_from
            Llio.string_from
            Llio.string_from))
      (Hashtbl.create 0)
      buf
  in
  let redis_lru_cache_eviction =
    maybe_from_buffer
      (Llio.option_from redis_lru_cache_eviction_from_buffer)
      None
      buf
  in
  { enable_auto_repair;
    auto_repair_timeout_seconds;
    auto_repair_disabled_nodes;
    enable_rebalance;
    cache_eviction_prefix_preset_pairs;
    redis_lru_cache_eviction;
  }

let to_buffer buf { enable_auto_repair;
                    auto_repair_timeout_seconds;
                    auto_repair_disabled_nodes;
                    enable_rebalance;
                    cache_eviction_prefix_preset_pairs;
                    redis_lru_cache_eviction;
                  } =
  Llio.int8_to buf 1;
  Llio.bool_to buf enable_auto_repair;
  Llio.float_to buf auto_repair_timeout_seconds;
  Llio.list_to Llio.string_to buf auto_repair_disabled_nodes;
  Llio.bool_to buf enable_rebalance;
  Llio.hashtbl_to
    Llio.string_to Llio.string_to
    buf
    cache_eviction_prefix_preset_pairs;
  Llio.option_to
    redis_lru_cache_eviction_to_buffer
    buf
    redis_lru_cache_eviction

module Update = struct
    type t = {
        enable_auto_repair' : bool option;
        auto_repair_timeout_seconds' : float option;
        auto_repair_add_disabled_nodes : string list;
        auto_repair_remove_disabled_nodes : string list;

        enable_rebalance' : bool option;

        add_cache_eviction_prefix_preset_pairs : (string * string) list;
        remove_cache_eviction_prefix_preset_pairs : string list;

        redis_lru_cache_eviction' : redis_lru_cache_eviction option;
      }

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let enable_auto_repair' = Llio.option_from Llio.bool_from buf in
      let auto_repair_timeout_seconds' = Llio.option_from Llio.float_from buf in
      let auto_repair_remove_disabled_nodes =
        Llio.list_from Llio.string_from buf in
      let auto_repair_add_disabled_nodes =
        Llio.list_from Llio.string_from buf in
      let enable_rebalance' = Llio.option_from Llio.bool_from buf in
      let add_cache_eviction_prefix_preset_pairs,
          remove_cache_eviction_prefix_preset_pairs =
        maybe_from_buffer
          (Llio.pair_from
             (Llio.list_from
                (Llio.pair_from
                   Llio.string_from
                   Llio.string_from))
             (Llio.list_from Llio.string_from))
          ([], [])
          buf
      in
      let redis_lru_cache_eviction' =
        maybe_from_buffer
          (Llio.option_from redis_lru_cache_eviction_from_buffer)
          None
          buf
      in
      { enable_auto_repair';
        auto_repair_timeout_seconds';
        auto_repair_remove_disabled_nodes;
        auto_repair_add_disabled_nodes;
        enable_rebalance';
        add_cache_eviction_prefix_preset_pairs;
        remove_cache_eviction_prefix_preset_pairs;
        redis_lru_cache_eviction';
      }

    let to_buffer buf { enable_auto_repair';
                        auto_repair_timeout_seconds';
                        auto_repair_remove_disabled_nodes;
                        auto_repair_add_disabled_nodes;
                        enable_rebalance';
                        add_cache_eviction_prefix_preset_pairs;
                        remove_cache_eviction_prefix_preset_pairs;
                        redis_lru_cache_eviction';
                      } =
      Llio.int8_to buf 1;
      Llio.option_to Llio.bool_to buf enable_auto_repair';
      Llio.option_to Llio.float_to buf auto_repair_timeout_seconds';
      Llio.list_to Llio.string_to buf auto_repair_remove_disabled_nodes;
      Llio.list_to Llio.string_to buf auto_repair_add_disabled_nodes;
      Llio.option_to Llio.bool_to buf enable_rebalance';
      Llio.list_to
        (Llio.pair_to Llio.string_to Llio.string_to)
        buf
        add_cache_eviction_prefix_preset_pairs;
      Llio.list_to Llio.string_to
        buf
        remove_cache_eviction_prefix_preset_pairs;
      Llio.option_to
        redis_lru_cache_eviction_to_buffer
        buf
        redis_lru_cache_eviction'

    let apply { enable_auto_repair;
                auto_repair_timeout_seconds;
                auto_repair_disabled_nodes;
                enable_rebalance;
                cache_eviction_prefix_preset_pairs;
                redis_lru_cache_eviction;
              }
              { enable_auto_repair';
                auto_repair_timeout_seconds';
                auto_repair_remove_disabled_nodes;
                auto_repair_add_disabled_nodes;
                enable_rebalance';
                add_cache_eviction_prefix_preset_pairs;
                remove_cache_eviction_prefix_preset_pairs;
                redis_lru_cache_eviction';
              }
      =
      { enable_auto_repair = Option.get_some_default
                               enable_auto_repair
                               enable_auto_repair';
        auto_repair_timeout_seconds = Option.get_some_default
                                        auto_repair_timeout_seconds
                                        auto_repair_timeout_seconds';
        auto_repair_disabled_nodes =
          List.filter
            (fun node ->
             not (List.mem node auto_repair_remove_disabled_nodes))
            (List.rev_append
               auto_repair_add_disabled_nodes
               auto_repair_disabled_nodes);
        enable_rebalance = Option.get_some_default
                             enable_rebalance
                             enable_rebalance';
        cache_eviction_prefix_preset_pairs =
          (let () =
             List.iter
               (fun prefix ->
                Hashtbl.remove cache_eviction_prefix_preset_pairs prefix)
               remove_cache_eviction_prefix_preset_pairs
           in
           let () =
             List.iter
               (fun (prefix, preset) ->
                Hashtbl.replace cache_eviction_prefix_preset_pairs prefix preset)
               add_cache_eviction_prefix_preset_pairs
           in
           cache_eviction_prefix_preset_pairs);

        redis_lru_cache_eviction =
          (match redis_lru_cache_eviction' with
           | Some x -> Some x
           | None -> redis_lru_cache_eviction);
      }
  end
