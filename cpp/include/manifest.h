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

namespace alba {
namespace proxy_protocol {
struct EncodingScheme {
  uint32_t k;
  uint32_t m;
  uint8_t w;
};

enum class compressor_t { NO_COMPRESSION, SNAPPY, BZIP2 };

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

enum class encryption_t { NO_ENCRYPTION };

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

typedef std::pair<boost::optional<uint32_t>, uint32_t> fragment_location_t;

template <class T> using layout = std::vector<std::vector<T>>;

struct lookup_result_t {
  uint32_t chunk_index;
  uint32_t fragment_index;
  uint32_t pos_in_fragment;
  uint32_t fragment_length;
  uint32_t fragment_version;
  uint32_t osd;

  lookup_result_t(uint32_t ci, uint32_t fi, uint32_t pif, uint32_t fl,
                  uint32_t fv, uint32_t osd)
      : chunk_index(ci), fragment_index(fi), pos_in_fragment(pif),
        fragment_length(fl), fragment_version(fv), osd(osd) {}
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

  boost::optional<lookup_result_t> to_chunk_fragment(uint32_t offset) const;

  Manifest() = default;
  Manifest &operator=(const Manifest &) = delete;
  Manifest(const Manifest &) = delete;
};

std::ostream &operator<<(std::ostream &, const EncodingScheme &);
std::ostream &operator<<(std::ostream &, const compressor_t &);
std::ostream &operator<<(std::ostream &, const Compression &);
std::ostream &operator<<(std::ostream &, const encryption_t &);
std::ostream &operator<<(std::ostream &, const EncryptInfo &);
std::ostream &operator<<(std::ostream &, const fragment_location_t &);
std::ostream &operator<<(std::ostream &, const Manifest &);
}
}
