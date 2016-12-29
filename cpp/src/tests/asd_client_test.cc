/*
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
*/

#include "asd_client.h"
#include "alba_common.h"
#include "tcp_transport.h"
#include "gtest/gtest.h"

#include <chrono>

// setup cpp -> schiet key in nen asd
// liefst met data die ik gemakkelijk kan verifieren?
// hmm, mss alba binary (of iets random), en die file dan hier ook lezen...
// hier partial read doen en zien

using std::string;
using std::vector;
using alba::byte;
using alba::transport::Transport;
using alba::transport::TCP_transport;
using alba::asd_protocol::slice;
using alba::asd_client::Asd_client;

TEST(asd_client, partial_read) {
  // der meerdere na elkaar doen! (om te zien of client niet in fucked state
  // komt

  string ip = getenv("ALBA_ASD_IP");
  string port = "8000";

  auto transport = std::unique_ptr<Transport>(
      new TCP_transport(ip, port, std::chrono::seconds(1)));

  Asd_client asd = Asd_client::make_client(std::move(transport), boost::none,
                                           std::chrono::seconds(1));

  slice slice1;
  byte target[50];
  slice1.offset = 0;
  slice1.length = 50;
  slice1.target = target;
  auto slices = vector<slice>{slice1};
  string key = "key1";

  asd.partial_get(key, slices);

  byte expected_target[50];
  memset(expected_target, (int)'a', 50);
  EXPECT_EQ(0, memcmp(target, expected_target, 50));

  memset(target, (int)'b', 50);

  asd.partial_get(key, slices);
  EXPECT_EQ(0, memcmp(target, expected_target, 50));
}
