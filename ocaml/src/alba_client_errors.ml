(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*)

open! Prelude

module Error = struct
  type t =
    | ChecksumMismatch
    | ChecksumAlgoNotAllowed
    | BadSliceLength
    | OverlappingSlices
    | SliceOutsideObject
    | FileNotFound
    | NoSatisfiablePolicy
    | NamespaceDoesNotExist
    | NotEnoughFragments
  [@@deriving show, enum]

  exception Exn of t

  let failwith t = raise (Exn t)
  let lwt_failwith t = Lwt.fail (Exn t)
end
