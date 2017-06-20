(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude
module Interval = struct
  type t =  {
    offset : Int64.t;
    length : int;
  } [@@deriving show]

  let overlap t1 t2 =
    let open Int64 in
    let next1 = add t1.offset (of_int t1.length)
    and next2 = add t2.offset (of_int t2.length) in
    not (next1 <=: t2.offset || next2 <=: t1.offset)

  let intersection t1 t2 =
    let open Int64 in
    let next1 = add t1.offset (of_int t1.length)
    and next2 = add t2.offset (of_int t2.length) in
    if (next1 <=: t2.offset || next2 <=: t1.offset)
    then None
    else begin
      let offset = Int64.max t1.offset t2.offset in
      Some { offset;
             length = Int64.(to_int (sub (min next1 next2) offset)); }
    end
end
