(*
Copyright 2015 Open vStorage NV

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

open Prelude

type t = {
    enable_auto_repair : bool;
    auto_repair_timeout_seconds : float;

    enable_rebalance : bool;
  } [@@deriving show, yojson]

let from_buffer buf =
  let ser_version = Llio.int8_from buf in
  assert (ser_version = 1);
  let enable_auto_repair = Llio.bool_from buf in
  let auto_repair_timeout_seconds = Llio.float_from buf in
  let enable_rebalance = Llio.bool_from buf in
  { enable_auto_repair;
    auto_repair_timeout_seconds;
    enable_rebalance;
  }

let to_buffer buf { enable_auto_repair;
                    auto_repair_timeout_seconds;
                    enable_rebalance; } =
  Llio.int8_to buf 1;
  Llio.bool_to buf enable_auto_repair;
  Llio.float_to buf auto_repair_timeout_seconds;
  Llio.bool_to buf enable_rebalance

module Update = struct
    type t = {
        enable_auto_repair' : bool option;
        auto_repair_timeout_seconds' : float option;

        enable_rebalance' : bool option;
      }

    let from_buffer buf =
      let ser_version = Llio.int8_from buf in
      assert (ser_version = 1);
      let enable_auto_repair' = Llio.option_from Llio.bool_from buf in
      let auto_repair_timeout_seconds' = Llio.option_from Llio.float_from buf in
      let enable_rebalance' = Llio.option_from Llio.bool_from buf in
      { enable_auto_repair';
        auto_repair_timeout_seconds';
        enable_rebalance';
      }

    let to_buffer buf { enable_auto_repair';
                        auto_repair_timeout_seconds';
                        enable_rebalance'; } =
      Llio.int8_to buf 1;
      Llio.option_to Llio.bool_to buf enable_auto_repair';
      Llio.option_to Llio.float_to buf auto_repair_timeout_seconds';
      Llio.option_to Llio.bool_to buf enable_rebalance'

    let apply { enable_auto_repair;
                auto_repair_timeout_seconds;
                enable_rebalance; }
              { enable_auto_repair';
                auto_repair_timeout_seconds';
                enable_rebalance'; }
      =
      { enable_auto_repair = Option.get_some_default
                               enable_auto_repair
                               enable_auto_repair';
        auto_repair_timeout_seconds = Option.get_some_default
                                        auto_repair_timeout_seconds
                                        auto_repair_timeout_seconds';
        enable_rebalance = Option.get_some_default
                             enable_rebalance
                             enable_rebalance'; }
  end
