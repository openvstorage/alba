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
    with _ -> None

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

  let minima ?(compare=compare) l =
    match l with
    | [] -> []
    | hd :: tl ->
       List.fold_left
         (fun (min, min_acc) item ->
           let res = compare min item in
           if res < 0
           then item, [ item; ]
           else if res = 0
           then min, (item :: min_acc)
           else min, min_acc)
         (hd, [ hd; ])
         tl
       |> snd

  let min ?compare l = minima ?compare l |> hd

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

  let split3 xs =
    let x0s_r, x1s_r, x2s_r =
      List.fold_left
        (fun (x0s,x1s,x2s) (x0,x1,x2) -> (x0::x0s, x1::x1s, x2::x2s))
        ([], [], []) xs
    in List.rev x0s_r, List.rev x1s_r, List.rev x2s_r

  let rev_map3 f l1 l2 l3 =
    let rec inner acc = function
      | [], [], [] -> acc
      | e1::l1, e2::l2, e3::l3 ->
         inner (f e1 e2 e3 :: acc) (l1, l2, l3)
      | _ -> invalid_arg "List.rev_map3"
    in
    inner [] (l1, l2, l3)

  let map3 f l1 l2 l3 =
    rev_map3 f (rev l1) (rev l2) (rev l3)

  let combine3 l1 l2 l3 = map3 (fun e1 e2 e3 -> e1, e2, e3) l1 l2 l3
end

module Option = struct
  type 'a t = 'a option

  let some x = Some x

  let map f = function
    | None -> None
    | Some a -> Some (f a)

  let lwt_map f = function
    | None -> Lwt.return_none
    | Some a ->
       let open Lwt.Infix in
       f a >>= fun r ->
       Lwt.return (Some r)

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

external set64_prim' : Lwt_bytes.t -> int -> int64 -> unit = "%caml_bigstring_set64"
external get64_prim' : Lwt_bytes.t -> int -> int64 = "%caml_bigstring_get64"

let deserialize ?(offset=0) deserializer s =
  deserializer (Llio.make_buffer s offset)

let serialize ?(buf_size=20) serializer a =
  let buf = Buffer.create buf_size in
  let () = serializer buf a in
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

let maybe_from_buffer a_from default buf =
  if Llio.buffer_done buf
  then default
  else a_from buf

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

module Result =
  struct
    include Result
    type ('a, 'b) t = ('a, 'b) result

    let from_buffer ok_from err_from buf =
      match Llio.int8_from buf with
      | 1 -> Ok (ok_from buf)
      | 2 -> Error (err_from buf)
      | k -> raise_bad_tag "Result.t" k

    let to_buffer ok_to err_to buf = function
      | Ok ok -> Llio.int8_to buf 1;
                 ok_to buf ok
      | Error err -> Llio.int8_to buf 2;
                     err_to buf err
  end

module Hashtbl = struct
  include Hashtbl

  exception Break

  let _choose ~n t =
    let res = ref None in
    let cntr = ref 0 in
    let () =
      try
        Hashtbl.iter
          (fun k v ->
            if n = !cntr
            then
              begin
                res := Some (k, v);
                raise Break
              end
            else
              incr cntr)
          t
      with Break -> () in
    !res

  let choose_first t = _choose ~n:0 t

  let choose_random t =
    let len = length t in
    if len = 0
    then None
    else _choose ~n:(Random.int len) t

  let map f t =
    Hashtbl.fold
      (fun k v acc ->
       (f k v) :: acc)
      t
      []

  let map_filter f t =
    Hashtbl.fold
      (fun k v acc ->
       match f k v with
       | None -> acc
       | Some x -> x :: acc)
      t
      []

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

  let keys h = Hashtbl.fold (fun k _ acc -> k :: acc) h []

  let values h = Hashtbl.fold (fun _ v acc -> v :: acc) h []

  let to_yojson _ value_to_yojson h =
    to_assoc_list h
    |> List.map (fun (k, v) -> k, value_to_yojson v)
    |> (fun x -> `Assoc x)

  let of_yojson _ value_of_yojson = function
    | `Assoc x ->
       List.map (fun (k, v) -> k,
                               match value_of_yojson v with
                               | Result.Ok v -> v
                               | Result.Error _ -> assert false)
                x
       |> from_assoc_list
       |> fun x -> Result.Ok x
    | _ -> Result.Error "nie goe"
end

module IntSet = Set.Make(struct type t = int let compare = compare end)
module Int64Set = Set.Make(Int64)
module StringSet = Set.Make(String)

module IntMap = Map.Make(struct type t = int let compare = compare end)
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

(* a call with return value has_more may return less than the
 * requested amount of objects/namespaces. when all values in
 * the range have been delivered has_more=false, otherwise
 * has_more=true. *)
type has_more = bool
type 'a counted_list = (int * 'a list) [@@deriving show]
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
    Result.Ok fs
  | e ->
    Result.Error (Yojson.Safe.to_string e)

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

module Url = struct

  type arakoon_cfg = Arakoon_config_url.arakoon_cfg =
    { cluster_id : string;
      key : string;
      ini_location : string }
      [@@deriving show]

  type t = Arakoon_config_url.url =
         | File of string
         | Etcd of (((string * int) list) * string)
         | Arakoon of arakoon_cfg
                        [@@deriving show]

  let make = Arakoon_config_url.make
end

let make_first_last_reverse () =
  let reverse = Random.bool ()
  and border = get_random_string 32 in
  let first, last =
    if reverse
    then ""    , Some (border,true)
    else border, None
  in
  first, last, reverse


let x_int64_to buf i =
  if i < (Int64.of_int32 Int32.max_int)
  then Llio.int32_to buf (Int64.to_int32 i)
  else
    begin
      Llio.int32_to buf Int32.max_int;
      Llio.int64_to buf i
    end

let x_int64_from buf =
  let i = Llio.int32_from buf in
  if i < Int32.max_int
  then Int64.of_int32 i
  else Llio.int64_from buf

let x_int64_be_to buf i =
  if i < (Int64.of_int32 Int32.max_int)
  then Llio.int32_be_to buf (Int64.to_int32 i)
  else
    begin
      Llio.int32_be_to buf Int32.max_int;
      Llio.int64_be_to buf i
    end

let x_int64_be_from buf =
  let i = Llio.int32_be_from buf in
  if i < Int32.max_int
  then Int64.of_int32 i
  else Llio.int64_be_from buf
