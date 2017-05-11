(*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
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
