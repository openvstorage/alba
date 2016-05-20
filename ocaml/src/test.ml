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
    Asd_config_test.suite;
    Asd_test.suite;
    "alba_crc32c" >::: Alba_crc32c_test.suite;
    Compressors_test.suite;
    "cache" >::: Cache_test.suite;
    Fragment_cache_test.suite;
    Gcrypt_test.suite;
    Rebalancing_helper_test.suite;
    Maintenance_test.suite;
    Erasure_test.suite;
    Buffer_pool_test.suite;
    Maintenance_coordination_test.suite;
    Posix_test.suite;
    Fragment_cache_config_test.suite;
  ]
