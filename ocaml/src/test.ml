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

open OUnit

let suite = "all" >:::[
    Prelude_test.suite;
    "Choose_test" >::: Choose_test.suite;
    Nsm_protocol_test.suite;
    Nsm_model_test.suite;
    Albamgr_protocol_test.suite;
    Albamgr_test.suite;
    Proxy_test.suite;
    Alba_test.suite;
    Disk_safety_test.suite;
    Asd_test.suite;
    "alba_crc32c" >::: Alba_crc32c_test.suite;
    Compressors_test.suite;
    "cache" >::: Cache_test.suite;
    Fragment_cache_test.suite;
    Gcrypt_test.suite;
    Maintenance_test.suite;
    Erasure_test.suite;
    Buffer_pool_test.suite;
    Maintenance_coordination_test.suite;
    (* Keystone_test.suite; *)
  ]
