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

#include "alba_common.h"
#include "checksum.h"
#include <boost/optional.hpp>
#include <iostream>
#include <map>
#include <memory>

namespace alba {
namespace proxy_protocol {
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

enum class encryption_t { NO_ENCRYPTION, ALGO_WITH_KEY };

class EncryptInfo {
public:
  virtual encryption_t get_encryption() const = 0;
  virtual void print(std::ostream &os) const = 0;

  virtual ~EncryptInfo(){};
};

class NoEncryption : public EncryptInfo {
  virtual encryption_t get_encryption() const {
    return encryption_t::NO_ENCRYPTION;
  }

  virtual void print(std::ostream &os) const { os << "NoEncryption()"; }
};

enum class algo_t { AES };
enum class chaining_mode_t { CBC, CTR };
enum class key_length_t { L256 };

class AlgoWithKey : public EncryptInfo {
  virtual encryption_t get_encryption() const {
    return encryption_t::ALGO_WITH_KEY;
  }

  virtual void print(std::ostream &os) const { os << "AlgoWithKey()"; }

public:
  algo_t algo = algo_t::AES;
  chaining_mode_t mode = chaining_mode_t::CTR;
  key_length_t key_length = key_length_t::L256;
  std::string key;
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
  // should be more fine grained about encryption when we want to support rora
  // for CTR mode encryption
  bool uses_encryption;
};

struct Manifest {
  std::string name;
  std::string object_id;
  std::vector<uint32_t> chunk_sizes;
  EncodingScheme encoding_scheme;

  std::unique_ptr<Compression> compression;
  std::unique_ptr<EncryptInfo> encrypt_info;
  std::unique_ptr<alba::Checksum> checksum;
  uint64_t size;
  layout<fragment_location_t> fragment_locations;
  layout<std::shared_ptr<alba::Checksum>> fragment_checksums;
  layout<uint32_t> fragment_packed_sizes;
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

std::ostream &operator<<(std::ostream &, const EncodingScheme &);
std::ostream &operator<<(std::ostream &, const compressor_t &);
std::ostream &operator<<(std::ostream &, const Compression &);
std::ostream &operator<<(std::ostream &, const encryption_t &);
std::ostream &operator<<(std::ostream &, const EncryptInfo &);
std::ostream &operator<<(std::ostream &, const fragment_location_t &);
std::ostream &operator<<(std::ostream &, const Manifest &);
std::ostream &operator<<(std::ostream &, const ManifestWithNamespaceId &);
std::ostream &operator<<(std::ostream &, const algo_t &);
std::ostream &operator<<(std::ostream &, const chaining_mode_t &);
}
}
