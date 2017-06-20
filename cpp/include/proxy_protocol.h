/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/

#pragma once

#include "checksum.h"
#include "llio.h"
#include "manifest.h"
#include "osd_info.h"
#include "proxy_sequences.h"

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
  PROTOCOL_VERSION_MISMATCH = 17,
  ASSERT_FAILED = 18
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

std::ostream &operator<<(std::ostream &, const SliceDescriptor &);
std::ostream &operator<<(std::ostream &, const ObjectSlices &);

typedef std::tuple<std::string, alba_id_t,
                   std::unique_ptr<ManifestWithNamespaceId>>
    object_info;

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
                                    ManifestWithNamespaceId &mf);

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

void write_read_objects_slices2_request(
    message_builder &mb, const string &namespace_,
    const std::vector<ObjectSlices> &object_slices, const bool allow_cached);

void read_read_objects_slices_response(message &m, Status &status,
                                       const std::vector<ObjectSlices> &dest);
void read_read_objects_slices2_response(message &m, Status &status,
                                        const std::vector<ObjectSlices> &dest,
                                        std::vector<object_info> &object_infos);

void write_update_session_request(
    message_builder &mb,
    const std::vector<std::pair<std::string, boost::optional<std::string>>>
        &args);

void read_update_session_response(
    message &m, Status &status,
    std::vector<std::pair<std::string, std::string>> &processed_kvs);

void write_apply_sequence_request(
    message_builder &mb, const string &namespace_, const bool write_barrier,
    const std::vector<std::shared_ptr<alba::proxy_client::sequences::Assert>>
        &asserts,
    const std::vector<std::shared_ptr<alba::proxy_client::sequences::Update>>
        &updates);

void read_apply_sequence_response(message &m, Status &status,
                                  std::vector<object_info> &);

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

void read_osd_info_response(message &m, Status &status, osd_map_t &result);

void write_osd_info2_request(message_builder &mb);

void read_osd_info2_response(message &m, Status &status, osd_maps_t &result);

void write_has_local_fragment_cache_request(message_builder &mb);
void read_has_local_fragment_cache_response(message &m, Status &status, bool &);

void write_get_fragment_encryption_key_request(message_builder &mb,
                                               const string &alba_id,
                                               const namespace_t namespace_id);
void read_get_fragment_encryption_key_response(
    message &m, Status &status, boost::optional<string> &enc_key);
}
}
