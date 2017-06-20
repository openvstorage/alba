/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/


#pragma once

#include "alba_common.h"
#include "checksum.h"
#include "encryption.h"

#include <boost/optional.hpp>
#include <iostream>
#include <map>
#include <memory>

namespace alba {
namespace proxy_protocol {

using alba::encryption::EncryptInfo;
using alba::encryption::encryption_t;

struct EncodingScheme {
  uint32_t k;
  uint32_t m;
  uint8_t w;
};

enum class compressor_t { NO_COMPRESSION, SNAPPY, BZIP2, TEST };

class Compression {
public:
  virtual compressor_t get_compressor() const = 0;
  virtual void print(std::ostream &os) const = 0;
  virtual ~Compression(){};
};

class NoCompression : public Compression {
  virtual compressor_t get_compressor() const {
    return compressor_t::NO_COMPRESSION;
  }
  virtual void print(std::ostream &os) const { os << "NoCompression()"; }
};

class SnappyCompression : public Compression {
  virtual compressor_t get_compressor() const { return compressor_t::SNAPPY; }
  virtual void print(std::ostream &os) const { os << "SnappyCompression()"; }
};

class BZip2Compression : public Compression {
  virtual compressor_t get_compressor() const { return compressor_t::BZIP2; }
  virtual void print(std::ostream &os) const { os << "BZip2Compression()"; }
};

class TestCompression : public Compression {
  virtual compressor_t get_compressor() const { return compressor_t::TEST; }
  virtual void print(std::ostream &os) const { os << "TestCompression()"; }
};

typedef std::pair<boost::optional<osd_t>, uint32_t> fragment_location_t;

template <class T> using layout = std::vector<std::vector<T>>;

struct Location {
  namespace_t namespace_id;
  std::string object_id;
  uint32_t chunk_id;
  uint32_t fragment_id;
  uint32_t offset;
  uint32_t length;
  fragment_location_t fragment_location;

  bool uses_compression;
  std::shared_ptr<EncryptInfo> encrypt_info;
  boost::optional<std::string> ctr;
};

struct Fragment {
  fragment_location_t loc;
  std::shared_ptr<alba::Checksum> crc;
  uint32_t len;
  boost::optional<std::string> ctr;
  boost::optional<std::string> fnr;
  Fragment() = default;
  Fragment &operator=(const Fragment &) = delete;
  Fragment(const Fragment &) = delete;
};

struct Manifest {
  std::string name;
  std::string object_id;
  std::vector<uint32_t> chunk_sizes;
  EncodingScheme encoding_scheme;

  std::unique_ptr<Compression> compression;
  std::shared_ptr<EncryptInfo> encrypt_info;
  std::unique_ptr<alba::Checksum> checksum;
  uint64_t size;
  layout<std::shared_ptr<Fragment>> fragments;
  uint32_t version_id;
  uint32_t max_disks_per_node;
  double timestamp = 1.0;

  Manifest() = default;
  Manifest &operator=(const Manifest &) = delete;
  Manifest(const Manifest &) = delete;
};

struct ManifestWithNamespaceId : Manifest {
  namespace_t namespace_id;
  ManifestWithNamespaceId() = default;
  ManifestWithNamespaceId &operator=(const ManifestWithNamespaceId &) = delete;
  ManifestWithNamespaceId(const ManifestWithNamespaceId &) = delete;
};

void dump_string(std::ostream &, const std::string &);
void dump_string_option(std::ostream &, const boost::optional<std::string> &);

std::ostream &operator<<(std::ostream &, const EncodingScheme &);
std::ostream &operator<<(std::ostream &, const compressor_t &);
std::ostream &operator<<(std::ostream &, const Compression &);
std::ostream &operator<<(std::ostream &, const fragment_location_t &);
std::ostream &operator<<(std::ostream &, const Fragment &);
std::ostream &operator<<(std::ostream &, const Manifest &);
std::ostream &operator<<(std::ostream &, const ManifestWithNamespaceId &);
}
}
