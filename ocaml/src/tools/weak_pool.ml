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

type 'a t = { mutable items : 'a Weak.t;
              make_element  : unit -> 'a; }

let create ?(initial_size=16) make_element =
  { items = Weak.create initial_size;
    make_element; }

let take t =
  let len = Weak.length t.items in
  let rec inner = function
    | n when n = len ->
       t.make_element ()
    | n ->
       begin
         match Weak.get t.items n with
         | None -> inner (n + 1)
         | Some item ->
            Weak.set t.items n None;
            item
       end
  in
  inner 0

let return t item =
  let len = Weak.length t.items in
  let rec inner = function
    | n when n = len ->
       let new_items = Weak.create (len * 2) in
       Weak.blit t.items 0 new_items 0 len;
       t.items <- new_items;
       Weak.set t.items n (Some item)
    | n ->
       if Weak.check t.items n
       then inner (n + 1)
       else Weak.set t.items n (Some item)
  in
  inner 0
