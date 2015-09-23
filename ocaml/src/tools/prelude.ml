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

module Compare = struct
  type t = LT | EQ | GT [@@deriving show]
end

module CompareLib = struct

  open Compare

  module type S = sig
    type t
    val compare' : t -> t -> Compare.t
  end

  module Make = functor(S : S) -> struct
    let (<:) a b = match S.compare' a b with
      | LT -> true
      | EQ | GT -> false
    let (>:) a b = match S.compare' a b with
      | GT -> true
      | EQ | LT -> false
    let (<=:) a b = match S.compare' a b with
      | LT | EQ -> true
      | GT -> false
    let (>=:) a b = match S.compare' a b with
      | GT | EQ -> true
      | LT -> false
    let (=:) a b = match S.compare' a b with
      | EQ -> true
      | LT | GT -> false
    let (<>:) a b = match S.compare' a b with
      | EQ -> false
      | LT | GT -> true

    let compare' = S.compare'

    let max a b =
      if a >: b
      then a
      else b

    let min a b =
      if a >: b
      then b
      else a
  end

  let wrap cmp a b = match cmp a b with
    | -1 -> LT
    | 0 -> EQ
    | 1 -> GT
    | _ -> failwith "Impossible compare result"

  module type T = sig
    type t
    val compare : t -> t -> int
  end

  module Default = functor(T : T) -> struct
    let compare' = wrap T.compare

    module CM = Make(struct
        type t = T.t
        let compare' = compare'
      end)
    include CM
  end

  module type U = sig
    type s
  end

  module Default' = functor(U : U) -> struct
    let compare' = wrap compare

    module CM = Make(struct
        type t = U.s
        let compare' = compare'
      end)
    include CM
  end
end

module String = struct
  include String

  module C = CompareLib.Default(String)
  include C
end

module Int = struct
  type t = int

  module C = CompareLib.Default'(struct type s = int end)
  include C

  let range =
    let rec inner acc low high =
      if low < high
      then inner (high - 1 :: acc) low (high - 1)
      else acc in
    inner []
end

module Int32 = struct
  include Int32

  module C = CompareLib.Default(Int32)
  include C
end

module Int64 = struct
  include Int64

  module C = CompareLib.Default(Int64)
  include C
end

module List = struct
  include List

  let any = function
    | [] -> false
    | _ -> true

  let hd = function
    | [] -> None
    | hd :: _ -> Some hd

  let hd_exn = function
    | [] -> failwith "hd_exn on empty list"
    | hd :: _ -> hd

  let tl_exn = tl

  let tl = function
    | [] -> None
    | _ :: tl -> Some tl

  let rec last = function
    | [] -> None
    | [a] -> Some a
    | _ :: tl -> last tl

  let rec last_exn = function
    | [] -> failwith "last_exn on empty list"
    | [a] -> a
    | _ :: tl -> last_exn tl

  let nth_exn = nth
  let nth l n =
    try Some (nth l n)
    with
    | Failure "nth"
    | Invalid_argument "List.nth" -> None

  let find_exn = find
  let find f l =
    try Some (find_exn f l)
    with Not_found -> None

  let rec find' f = function
    | [] -> None
    | hd :: tl ->
      begin match f hd with
        | None -> find' f tl
        | (Some _) as i -> i
      end

  let rec find_index' f pos = function
    | [] -> None
    | hd :: tl ->
      if f hd
      then Some pos
      else find_index' f (pos+1) tl

  let find_index f l = find_index' f 0 l

  let flatten_unordered lists =
    let rec inner acc = function
      | [] -> acc
      | list :: lists -> inner (List.rev_append list acc) lists in
    inner [] lists

  let flatmap f l =
    flatten (map f l)

  let flatmap_unordered f l =
    flatten_unordered (map f l)

  let group_by f l =
    let h = Hashtbl.create 3 in
    List.iter
      (fun item ->
         let key = f item in
         let items = try Hashtbl.find h key with
           | Not_found -> []
         in
         Hashtbl.replace h key (item::items))
      l;
    h

  let rec drop l = function
    | 0 -> l
    | n -> drop (List.tl l) (n - 1)

  let min ?(compare=compare) l =
    let rec inner min = function
      | [] -> Some min
      | item::tl ->
        inner
          (if compare min item > 0
           then item
           else min)
          tl
    in
    match l with
    | [] -> None
    | hd::tl -> inner hd tl

  let max ?(compare=compare) l =
    min ~compare:(fun a b -> compare b a) l

  let take n l =
    let rec inner acc n xs =
      match (n,xs) with
      | 0,_ | _,[] -> List.rev acc
      | n, x :: xs -> inner (x::acc) (n-1) xs
    in
    inner [] n l

  let unfold f s0 =
    let rec inner acc = function
      | None -> List.rev acc
      | Some (el, s) ->
        inner (el :: acc) (f s)
    in
    inner [] (f s0)

  let map_filter_rev f l =
    let rec inner acc = function
      | [] -> acc
      | hd::tl ->
        match f hd with
        | None -> inner acc tl
        | Some el -> inner (el::acc) tl
    in
    inner [] l

  let map_filter f l =
    List.rev (map_filter_rev f l)

  let merge_head ?(compare= compare) x y max_n =
    let rec push x y n =
      let rec _inner r y = function
        | 0 -> r
        | n ->
           begin
             match y with
             | [] -> r
             | yh :: yt -> _inner (yh::r) yt (n-1)
           end
      in
      _inner x y n
    in
    let rec _inner todo acc x y =
      if todo = 0
      then acc
      else
        match x,y with
        |     [], []     -> acc
        |     [],  y     -> push acc y todo
        |      x, []     -> push acc x todo
        | xh::xt, yh::yt ->
           begin
             let todo' = todo - 1
             and c = compare xh yh in
             if c < 0      then  _inner todo' (xh::acc) xt y
             else if c = 0 then  _inner todo' (xh::acc) xt yt
             else                _inner todo' (yh::acc) x  yt
           end
    in
    (_inner max_n [] x y) |> List.rev
end

module Option = struct
  type 'a t = 'a option

  let map f = function
    | None -> None
    | Some a -> Some (f a)

  let iter f = function
    | None -> ()
    | Some a -> f a

  let show s = function
    | None -> "None"
    | Some a -> Printf.sprintf "Some(%s)" (s a)

  let get_some = function
    | Some a -> a
    | None -> failwith "None while Some was expected"

  let get_some_default default = function
    | Some a -> a
    | None -> default
end

type direction =
  | Left
  | Right

module Map = struct
  module Make(Ord : Map.OrderedType) = struct
    module M = Map.Make(Ord)
    include M

    (* define same types as in original Map implementation,
       this allows us to access their contents with Obj.magic *)
    type key' = Ord.t
    type 'a t' =
      | Empty
      | Node of 'a t' * key' * 'a * 'a t' * int

    let _cast (m : 'a t) : 'a t' = Obj.magic m
    let _uncast (m : 'a t') : 'a t = Obj.magic m

    let empty = _uncast Empty

    module Zipper = struct

      type 'a t =
        'a t' * ('a t' * direction) list

      let first ?(z=[]) t =
        let rec inner acc = function
          | Empty ->
            None
          | Node(Empty, _, _, _, _) as t ->
            Some (t, acc)
          | Node(l, _, _, _, _) as t ->
            inner ((t, Left)::acc) l in
        inner z (_cast t)

      let last ?(z=[]) t =
        let rec inner acc = function
          | Empty -> None
          | Node(_, _, _, Empty, _) as t ->
            Some (t, acc)
          | Node(_, _, _, r, _) as t ->
            inner ((t, Right) :: acc) r in
        inner z (_cast t)

      let get (h, _) =
        match h with
        | Empty ->
          failwith "invalid zipper"
        | Node(_, k, v, _, _) ->
          (k, v)

      let next (h, z) =
        match h with
        | Empty ->
          failwith "invalid zipper"
        | Node(_, _, _, Empty, _) ->
          begin
            let rec inner = function
              | [] -> None
              | (t, Left) :: z' -> Some (t, z')
              | (_, Right) :: z' -> inner z'
            in
            inner z
          end
        | Node(_, _, _, r, _) ->
          let z' = (h, Right) :: z in
          first ~z:z' (_uncast r)

      let prev (h, z) =
        match h with
        | Empty ->
          failwith "invalid zipper"
        | Node(Empty, _, _, _, _) ->
          begin
            let rec inner = function
              | (t, Right) :: z' ->
                Some (t, z')
              | (_t, Left) :: z' ->
                inner z'
              | [] ->
                None in
            inner z
          end
        | Node(l, _, _, _, _) ->
          let z' = (h, Left) :: z in
          last ~z:z' (_uncast l)

      let jump ~dir k t =
        let rec inner acc = function
          | Empty ->
            begin
              let rec inner = function
                | (t, d) :: tl when d <> dir ->
                  Some (t, tl)
                | _ :: tl ->
                  inner tl
                | [] ->
                  None in
              inner acc
            end
          | Node(l, k', _v, r, _) as t ->
            begin
              match Ord.compare k k' with
              | -1 ->
                inner ((t, Left) :: acc) l
              | 0 ->
                Some (t, acc)
              | 1 ->
                inner ((t, Right) :: acc) r
              | _ ->
                failwith "impossible compare result"
            end in
        inner [] (_cast t)
    end
  end
end

let () = Random.self_init ()

let compose f g x = f (g x)

let to_hex d =
  let size = String.length d in
  let r_size = size * 2 in
  let result = Bytes.create r_size in
  for i = 0 to (size - 1) do
    Bytes.blit (Printf.sprintf "%02x" (int_of_char d.[i])) 0 result (2*i) 2;
  done;
  result

let from_hex s =
  let len = String.length s in
  assert (len mod 2 = 0);
  let digit c =
    match c with
    | '0'..'9' -> Char.code c - Char.code '0'
    | 'A'..'F' -> Char.code c - Char.code 'A' + 10
    | 'a'..'f' -> Char.code c - Char.code 'a' + 10
    | _ -> raise (Invalid_argument "from_hex")
  in
  let byte i = digit s.[2*i] lsl 4 + digit s.[(2*i)+1] in
  Bytes.init (len / 2) (fun i -> Char.chr (byte i))


exception DecodingFailure of string

let raise_bad_tag hint k =
  let msg = Printf.sprintf "%s: invalid tag 0x%x" hint k in
  let exn = DecodingFailure msg in
  raise exn


external set32_prim : string -> int -> int32 -> unit = "%caml_string_set32"
external get32_prim : string -> int -> int32 = "%caml_string_get32"

external set32_prim' : Lwt_bytes.t -> int -> int32 -> unit = "%caml_bigstring_set32"
external get32_prim' : Lwt_bytes.t -> int -> int32 = "%caml_bigstring_get32"


let deserialize ?(offset=0) deserializer s =
  deserializer (Llio.make_buffer s offset)
let serialize ?(buf_size=20) serializer a =
  let buf = Buffer.create buf_size in
  serializer buf a;
  Buffer.contents buf

let serialize_with_length ?buf_size serializer a =
  let res =
    serialize
      ?buf_size
      (Llio.pair_to Llio.int_to serializer)
      (0,              (* temporarily put in length 0 *)
       a)
  in
  (* fill length in the first 4 bytes *)
  let len = String.length res - 4 in
  set32_prim res 0 (Int32.of_int len);
  res

let get_start_key i n =
  if i < 0 || i > n
  then failwith "bad input for get_start_key";

  if i = n
  then None
  else
    begin
      let s = i * (1 lsl 32) / n in
      Some (serialize Llio.int32_be_to (Int32.of_int s))
    end

module Hashtbl = struct
  include Hashtbl

  exception Break

  let choose t =
    let res = ref None in
    let () =
      try
        Hashtbl.iter
          (fun k v ->
             res := Some (k, v);
             raise Break) t
      with Break -> () in
    !res

  let from_assoc_list l =
    let h = Hashtbl.create 3 in
    List.iter
      (fun (k, v) -> Hashtbl.add h k v)
      l;
    h

  let to_assoc_list h =
    Hashtbl.fold
      (fun k v acc -> (k, v) :: acc)
      h
      []
end

module IntSet = Set.Make(struct type t = int let compare = compare end)
module Int32Set = Set.Make(Int32)
module Int64Set = Set.Make(Int64)
module StringSet = Set.Make(String)

module IntMap = Map.Make(struct type t = int let compare = compare end)
module Int32Map = Map.Make(Int32)
module StringMap = Map.Make(String)

let finalize f final =
  match f () with
  | exception exn ->
    final ();
    raise exn
  | res ->
    final ();
    res

let get_random_string len =
  Bytes.init len (fun _ -> Char.chr (Random.int 256))

module HexString = struct
  type t = string

  let show = to_hex

  let pp formatter t =
    Format.pp_print_string formatter (show t)
end

module HexInt32 = struct
  type t = int32

  let show = Printf.sprintf "0x%08lx"

  let pp formatter t =
    Format.pp_print_string formatter (show t)
end

type has_more = bool
type 'a counted_list = 'a Std.counted_list
type 'a counted_list_more = 'a counted_list * has_more

let counted_list_more_from a_from =
  Llio.pair_from
    (Llio.counted_list_from a_from)
    Llio.bool_from

let counted_list_more_to a_to =
  Llio.pair_to
    (Llio.counted_list_to a_to)
    Llio.bool_to

open Lwt.Infix

let list_all_x ~first get_first harvest =
  let rec inner cnt acc first finc =
    harvest ~first ~finc >>= fun ((cnt', items), has_more) ->
    let acc' = List.rev_append items acc in
    let cnt'' = cnt + cnt' in
    if has_more
    then inner cnt'' acc' (get_first (List.hd_exn acc')) false
    else Lwt.return (cnt'', List.rev acc')
  in
  inner 0 [] first true

type timestamp = float

let show_timestamp x =
      let open Unix in
      let t = localtime x in
      let s = (float_of_int t.tm_sec) +. (x -. (floor x)) in
      Printf.sprintf "%04i/%02i/%02i_%02i:%02i:%02.4f" (t.tm_year + 1900)
                     (t.tm_mon + 1)
                     t.tm_mday
                     t.tm_hour t.tm_min s

let pp_timestamp : Format.formatter -> timestamp -> unit =
  fun fmt timestamp ->
  Format.pp_print_string fmt (show_timestamp timestamp)

let timestamp_to_yojson t = `Float t
let timestamp_of_yojson = function
  | `Float fs ->
    `Ok fs
  | e ->
    `Error (Yojson.Safe.to_string e)

let _BATCH_SIZE = 200

let cap_max ?(cap=_BATCH_SIZE) ~max () =
  if max < 0 || max > cap
  then cap
  else max

module Lwt_list = struct
  include Lwt_list

  let find_s_exn = find_s
  let find_s f l =
    Lwt.catch
      (fun () -> Lwt.return (Some (find_s_exn f l)))
      (function
        | Not_found -> Lwt.return_none
        | exn -> Lwt.fail exn)
end

let with_timing f =
  let t0 = Unix.gettimeofday () in
  let res = f () in
  let t1 = Unix.gettimeofday () in
  t1 -. t0, res

let with_timing_lwt f =
  let t0 = Unix.gettimeofday () in
  let open Lwt.Infix in
  f () >>= fun res ->
  let t1 = Unix.gettimeofday () in
  Lwt.return (t1 -. t0, res)

module Error = struct
  type ('a, 'b) t =
    | Ok of 'a
    | Error of 'b

  let map f = function
    | Ok a -> Ok (f a)
    | Error _ as err -> err

  module Lwt = struct

    let return a = Lwt.return (Ok a)
    let fail b = Lwt.return (Error b)

    let bind t f =
      t >>= function
      | Ok a -> f a
      | (Error b as res) -> Lwt.return res

    let with_timing f =
      with_timing_lwt f >>= fun (delta, res) ->
      Lwt.return (map (fun a -> (delta, a)) res)
  end
end
