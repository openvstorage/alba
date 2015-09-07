(*
Copyright 2013-2015 Open vStorage NV

Licensed under the Open vStorage open source license (the "License"). This
License is entirely based on the Apache License, Version 2.0. You may not use
this file except in compliance with the License. You may obtain a copy of the
License at
    http://www.openvstorage.org/license
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
OR CONDITIONS OF ANY KIND, either express or implied.
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
