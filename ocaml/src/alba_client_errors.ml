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
