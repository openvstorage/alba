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

#pragma once

#include "llio.h"
#include "checksum.h"
#include "manifest.h"
#include "osd_info.h"

namespace alba {
namespace proxy_protocol {

enum return_code : uint32_t {
  OK = 0,
  UNKNOWN = 1,
  OVERWRITE_NOT_ALLOWED = 2,
  OBJECT_DOES_NOT_EXIST = 3,
  NAMESPACE_ALREADY_EXISTS = 4,
  NAMESPACE_DOES_NOT_EXIST = 5,
  //  ENCRYPTION_KEY_REQUIRED = 6,
  CHECKSUM_MISMATCH = 7,
  CHECKSUM_ALGO_NOT_ALLOWED = 8,
  PRESET_DOES_NOT_EXIST = 9,
  BAD_SLICE_LENGTH = 10,
  OVERLAPPING_SLICES = 11,
  SLICE_OUTSIDE_OBJECT = 12,
  UNKNOWN_OPERATION = 13,
  FILE_NOT_FOUND = 14,
  NO_SATISFIABLE_POLICY = 15,
  PROTOCOL_VERSION_MISMATCH = 17
};

struct Status {
  void set_rc(uint32_t return_code) { _return_code = return_code; }

  bool is_ok() const { return _return_code == (uint32_t)return_code::OK; }

  uint32_t _return_code;
  std::string _what;
};

struct SliceDescriptor {
  byte *buf;
  const uint64_t offset;
  const uint32_t size;
};

struct ObjectSlices {
  const std::string &object_name;
  const std::vector<SliceDescriptor> slices;
};

using std::string;
using boost::optional;
using llio::message_builder;
using llio::message;

void write_list_namespaces_request(message_builder &mb, const string &first,
                                   const bool finc,
                                   const optional<string> &last,
                                   const bool linc, const int max,
                                   const bool reverse);
void read_list_namespaces_response(message &m, Status &status,
                                   std::vector<string> &namespaces,
                                   bool &has_more);

void write_namespace_exists_request(message_builder &mb, const string &name);
void read_namespace_exists_response(message &m, Status &status, bool &exists);

void write_create_namespace_request(message_builder &mb, const string &name,
                                    const optional<string> &preset_name);
void read_create_namespace_response(message &m, Status &status);

void write_delete_namespace_request(message_builder &mb, const string &name);
void read_delete_namespace_response(message &m, Status &status);

void write_list_objects_request(message_builder &mb, const string &namespace_,
                                const string &first, const bool finc,
                                const optional<string> &last, const bool linc,
                                const int max, const bool reverse);
void read_list_objects_response(message &m, Status &status,
                                std::vector<string> &objects, bool &has_more);

void write_read_object_fs_request(message_builder &mb, const string &namespace_,
                                  const string &object_name,
                                  const string &dest_file,
                                  const bool consistent_read,
                                  const bool should_cache);
void read_read_object_fs_response(message &m, Status &status);

void write_write_object_fs_request(message_builder &mb,
                                   const string &namespace_,
                                   const string &object_name,
                                   const string &input_file,
                                   const bool allow_overwrite,
                                   const Checksum *checksum);
void read_write_object_fs_response(message &m, Status &status);

void write_write_object_fs2_request(message_builder &mb,
                                    const string &namespace_,
                                    const string &object_name,
                                    const string &input_file,
                                    const bool allow_overwrite,
                                    const Checksum *checksum);

void read_write_object_fs2_response(message &m, Status &status,
                                    Manifest &manifest);

void write_delete_object_request(message_builder &mb, const string &namespace_,
                                 const string &object_name,
                                 const bool may_not_exist);
void read_delete_object_response(message &m, Status &status);

void write_get_object_info_request(message_builder &mb,
                                   const string &namespace_,
                                   const string &object_name,
                                   const bool consistent_read,
                                   const bool should_cache);
void read_get_object_info_response(message &m, Status &status, uint64_t &size,
                                   Checksum *&checksum);

void write_read_objects_slices_request(
    message_builder &mb, const string &namespace_,
    const std::vector<ObjectSlices> &object_slices, const bool allow_cached);

void read_read_objects_slices_response(message &m, Status &status,
                                       const std::vector<ObjectSlices> &dest);

void write_invalidate_cache_request(message_builder &mb,
                                    const string &namespace_);

void read_invalidate_cache_response(message &m, Status &status);

void write_drop_cache_request(message_builder &mb, const string &namespace_);

void read_drop_cache_response(message &m, Status &status);

void write_get_proxy_version_request(message_builder &mb);
void read_get_proxy_version_response(message &m, Status &status, int32_t &major,
                                     int32_t &minor, int32_t &version,
                                     std::string &hash);
void write_ping_request(message_builder &mb, const double delay);
void read_ping_response(message &m, Status &status, double &timestamp);

void write_osd_info_request(message_builder &mb);
void read_osd_info_response(
    message &m, Status &status,
    std::vector<std::pair<osd_t, std::unique_ptr<OsdInfo>>> &result);
}
}
