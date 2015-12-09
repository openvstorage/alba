(*
Copyright 2015 iNuron NV

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

type value_source =
  | Fast
  | Slow
  | Stale
  [@@ deriving show]

module Cache = struct
type 'a node = {a: 'a ;
                prev : 'a node ref;
                next : 'a node ref;
               }

let make_node a =
  let rec n = { a;
                prev = ref n;
                next = ref n;
              }
  in
  n


type ('a,'b) t = {
    h : ('a, ('b * 'a node)) Hashtbl.t;
    front : 'a node;
    back: 'a node;
    mutable size : int;
    max_size : int;
    weight: 'b -> int;
  }

let unlink_node n =
  let pn = !(n.prev) in
  pn.next := !(n.next);
  let nn = !(n.next) in
  nn.prev := !(n.prev)

let insert_node cache n =
  let first = !(cache.front.next) in
  n.next := first;
  first.prev := n;
  n.prev := cache.front;
  cache.front.next := n

let make
      ?(weight = fun _ ->1)
      ?(max_size = 997) example_key  =
  let front = make_node example_key in
  let back = make_node example_key in
  let h = Hashtbl.create 97 in
  front.next := back;
  back.prev := front;
  { h ; front;back; size=0;max_size; weight }

let add t k v =
  try
    let old_v, n = Hashtbl.find t.h k in
    let () = Hashtbl.replace t.h k (v,n) in
    unlink_node n;
    insert_node t n;
    t.size <- t.size - t.weight old_v + t.weight v;
    (* we also could evict here *)
  with Not_found ->
    begin
      let n = make_node k in
      let () = Hashtbl.add t.h k (v,n) in
      insert_node t n;
      let w = t.weight v in
      while t.size + w > t.max_size do
        let victim = !(t.back.prev) in
        unlink_node victim;
        let victim_b,_ = Hashtbl.find t.h victim.a in
        let victim_w = t.weight victim_b in
        let () = Hashtbl.remove t.h victim.a in
        t.size <- t.size - victim_w
      done;
      t.size <- t.size + w
    end


let lookup cache k =
  try
    let v,n = Hashtbl.find cache.h k in
    unlink_node n;
    insert_node cache n;
    Some v
  with Not_found -> None

let clear cache =
  Hashtbl.clear cache.h;
  cache.size <- 0;
  cache.front.next := cache.back;
  cache.back.prev := cache.front

let order_next cache =
  let rec loop acc current =
    if !current == cache.back
    then List.rev acc
    else let cc = !current in
         let a = cc.a in
         loop (a::acc) cc.next
  in
  loop [] (cache.front.next)

let order_prev cache =
  let rec loop acc current =
    if !current == cache.front
    then acc
    else let cc = !current in
         let a = cc.a in
         loop (a::acc) cc.prev
  in
  loop [] cache.back.prev

let remove cache k =
  match Hashtbl.find cache.h k with
  | exception Not_found -> ()
  | (_, n) ->
    unlink_node n;
    Hashtbl.remove cache.h k
end

module ACache (* ;) *)= (Cache : sig
  type ('a,'b ) t
  val lookup : ('a,'b) t -> 'a -> 'b option
  val make : ?weight:('b -> int) -> ?max_size:int -> 'a -> ('a,'b) t
  val add : ('a,'b) t -> 'a -> 'b -> unit
  val remove : ('a, 'b) t -> 'a -> unit
  val clear : ('a,'b) t  -> unit
end)
