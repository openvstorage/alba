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

#include "llio.h"
#include "proxy_protocol.h"
#include "snappy.h"
#include "stuff.h"
#include <boost/optional/optional_io.hpp>

namespace alba {
namespace llio {

using namespace proxy_protocol;
template <> void from(message &m, EncodingScheme &es) {
  uint8_t version;
  from(m, version);
  if (version != 1) {
    throw deserialisation_exception("unexpected EncodingScheme version");
  }

  from(m, es.k);
  from(m, es.m);
  from(m, es.w);
}

void from(message &m, std::unique_ptr<Compression> &p) {
  uint8_t type;
  from(m, type);
  Compression *r;
  switch (type) {
  case 1: {
    r = new NoCompression();
  }; break;
  case 2: {
    r = new SnappyCompression();
  }; break;
  case 3: {
    r = new BZip2Compression();
  }; break;
  case 4: {
    r = new TestCompression();
  }; break;
  default: {
    ALBA_LOG(WARNING, "unknown compression type " << (int)type);
    throw deserialisation_exception("unknown compression type");
  };
  }
  p.reset(r);
}

void from(message &m, chaining_mode_t &mode) {
  uint8_t type;
  from(m, type);
  switch (type) {
  case 1: {
    mode = chaining_mode_t::CBC;
  }; break;
  case 2: {
    mode = chaining_mode_t::CTR;
  }; break;
  default: { throw deserialisation_exception("unknown chaining_mode"); };
  }
}

void from(message &m, key_length_t &kl) {
  uint8_t type;
  from(m, type);
  switch (type) {
  case 1: {
    kl = key_length_t::L256;
  } break;
  default: { throw deserialisation_exception("unknown key_length"); };
  }
}

void from(message &m, proxy_protocol::algo_t &algo) {
  uint8_t type;
  from(m, type);
  switch (type) {
  case 1: {
    algo = proxy_protocol::algo_t::AES;
  }; break;
  default: { throw deserialisation_exception("unknown algo"); };
  }
}
void from(message &m, Encrypted &awk) {

  // ALBA_LOG(DEBUG, "pos = " << m.get_pos());
  // alba::stuff::dump_buffer(std::cout, m.current(16), 16);

  // AES   CTR  KEY_LENGTH  x length of key  key bytes
  // 01    02   01         01 20 00 00 00    fa c8
  from(m, awk.algo);
  from(m, awk.mode);
  from(m, awk.key_length);

  // key_identification
  uint8_t tag;
  from(m, tag);
  assert(tag == 1); // KeySha256
  from(m, awk.key_identification);
  if (awk.key_identification.size() != 32) {
    throw deserialisation_exception("key length != 32");
  }
}

void from(message &m, std::unique_ptr<EncryptInfo> &p) {
  uint8_t type;
  from(m, type);
  EncryptInfo *r;
  switch (type) {
  case 1: {
    r = new NoEncryption();
  }; break;
  case 2: {
    Encrypted *awk = new Encrypted();
    from(m, *awk);
    r = awk;
  }; break;
  default: {
    ALBA_LOG(WARNING, "unknown encryption scheme: type=" << type);
    throw deserialisation_exception("unknown encryption scheme)");
  };
  }
  p.reset(r);
}

template <> void from2(message &m, Manifest &mf, bool &ok_to_continue) {
  ok_to_continue = false;
  uint8_t version;
  from(m, version);
  if (version != 1) {
    throw deserialisation_exception("unexpecteded Manifest version");
  }

  std::string compressed;
  from(m, compressed);

  std::string real;
  snappy::Uncompress(compressed.data(), compressed.size(), &real);
  std::vector<char> buffer(real.begin(), real.end());
  ok_to_continue = true;
  message m2(buffer);
  from(m2, mf.name);
  from(m2, mf.object_id);

  std::vector<uint32_t> chunk_sizes;
  from(m2, mf.chunk_sizes);

  uint8_t version2;
  from(m2, version2);
  if (version2 != 1) {
    throw deserialisation_exception("unexpected version2");
  }

  from(m2, mf.encoding_scheme);

  from(m2, mf.compression);

  from(m2, mf.encrypt_info);
  from(m2, mf.checksum);
  from(m2, mf.size);
  uint8_t layout_tag;
  from(m2, layout_tag);
  if (layout_tag != 1) {
    throw deserialisation_exception("unexpected layout tag");
  }
  from(m2, mf.fragment_locations);

  uint8_t layout_tag2;
  from(m2, layout_tag2);
  if (layout_tag2 != 1) {
    throw deserialisation_exception("unexpected layout tag 2");
  }

  // from(m2, mf.fragment_checksums); // TODO: how to this via the layout based
  // template ?
  // iso this:

  uint32_t n_chunks;
  from(m2, n_chunks);

  for (uint32_t i = 0; i < n_chunks; i++) {
    uint32_t n_fragments;
    from(m2, n_fragments);
    std::vector<std::shared_ptr<alba::Checksum>> chunk(n_fragments);
    for (int32_t f = n_fragments - 1; f >= 0; --f) {
      alba::Checksum *p;
      from(m2, p);
      std::shared_ptr<alba::Checksum> sp(p);
      chunk[f] = sp;
    };
    mf.fragment_checksums.push_back(chunk);
  }

  uint8_t layout_tag3;
  from(m2, layout_tag3);
  if (layout_tag3 != 1) {
    throw deserialisation_exception("unexpected layout tag 3");
  }

  from(m2, mf.fragment_packed_sizes);

  from(m2, mf.version_id);
  from(m2, mf.max_disks_per_node);
  from(m2, mf.timestamp);
}

template <> void from(message &m, Manifest &mf) {
  bool dont_care = false;
  from2(m, mf, dont_care);
}

template <>
void from2(message &m, ManifestWithNamespaceId &mfid, bool &ok_to_continue) {
  try {
    from2(m, (Manifest &)mfid, ok_to_continue);
    from(m, mfid.namespace_id);
  } catch (deserialisation_exception &e) {
    if (ok_to_continue) {
      from(m, mfid.namespace_id);
    };
    throw;
  }
}

template <> void from(message &m, ManifestWithNamespaceId &mfid) {
  bool dont_care = false;
  from2(m, mfid, dont_care);
}
}

namespace proxy_protocol {

std::ostream &operator<<(std::ostream &os, const EncodingScheme &scheme) {
  os << "EncodingScheme{k=" << scheme.k << ", m=" << scheme.m
     << ", w=" << (int)scheme.w << "}";

  return os;
}

std::ostream &operator<<(std::ostream &os, const compressor_t &compressor) {
  switch (compressor) {
  case compressor_t::NO_COMPRESSION:
    os << "NO_COMPRESSION";
    break;
  case compressor_t::SNAPPY:
    os << "SNAPPY";
    break;
  case compressor_t::BZIP2:
    os << "BZIP2";
    break;
  case compressor_t::TEST:
    os << "TEST";
    break;
  };
  return os;
}

std::ostream &operator<<(std::ostream &os, const algo_t &algo) {
  switch (algo) {
  case algo_t::AES: {
    os << "AES";
  }
  };
  return os;
}

std::ostream &operator<<(std::ostream &os, const chaining_mode_t &mode) {
  switch (mode) {
  case chaining_mode_t::CBC: {
    os << "CBC";
  }; break;
  case chaining_mode_t::CTR: {
    os << "CTR";
  }; break;
  };
  return os;
}

std::ostream &operator<<(std::ostream &os, const Compression &c) {
  c.print(os);
  return os;
}

std::ostream &operator<<(std::ostream &os, const encryption_t &encryption) {
  switch (encryption) {
  case encryption_t::NO_ENCRYPTION:
    os << "NO_ENCRYPTION";
    break;
  case encryption_t::ENCRYPTED:
    os << "ENCRYPTED";
  default:
    os << "?encryption?";
  };
  return os;
}

std::ostream &operator<<(std::ostream &os, const EncryptInfo &info) {
  info.print(os);
  return os;
}

std::ostream &operator<<(std::ostream &os, const fragment_location_t &f) {
  os << "(" << f.first // boost knows how
     << ", " << f.second << ")";
  return os;
}

void dump_string(std::ostream &os, const std::string &s) {
  const char *bytes = s.data();
  const int size = s.size();
  stuff::dump_buffer(os, bytes, size);
}
std::ostream &operator<<(std::ostream &os, const Manifest &mf) {
  using alba::stuff::operator<<;
  os << "{"
     << "name = `";
  dump_string(os, mf.name);
  os << "`, " << std::endl;
  os << "  object_id = `";
  dump_string(os, mf.object_id);
  os << "`, " << std::endl

     << "  encoding_scheme = " << mf.encoding_scheme << "," << std::endl
     << "  compression = " << *mf.compression << "," << std::endl
     << "  encryptinfo = " << *mf.encrypt_info << "," // dangerous
     << "  chunk_sizes = " << mf.chunk_sizes << "," << std::endl
     << "  size = " << mf.size << std::endl
     << std::endl
     << "  checksum= " << *mf.checksum << "," << std::endl
     << "  fragment_locations = " << mf.fragment_locations << "," << std::endl
     << "  fragment_checksums = " << mf.fragment_checksums << "," << std::endl
     << "  fragment_packed_sizes = [" << std::endl;

  for (const std::vector<uint32_t> &c : mf.fragment_packed_sizes) {
    os << c << "," << std::endl;
  }
  os << "  ], ";
  os << std::endl
     << "  version_id = " << mf.version_id << "," << std::endl
     << "  timestamp = " << mf.timestamp // TODO: decent formatting?
     << "}";
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const ManifestWithNamespaceId &mfid) {
  os << "{" << (Manifest &)mfid << ", namespace_id = " << mfid.namespace_id
     << "} ";
  return os;
}
}
}
