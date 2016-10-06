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

template <> void from(message &m, proxy_protocol::EncodingScheme &es) {
  uint8_t version;
  from(m, version);
  if (version != 1) {
    throw deserialisation_exception("unexpected EncodingScheme version");
  }

  from(m, es.k);
  from(m, es.m);
  from(m, es.w);
}

void from(message &m, std::unique_ptr<proxy_protocol::Compression> &p) {
  uint8_t type;
  from(m, type);
  proxy_protocol::Compression *r;
  switch (type) {
  case 1: {
    r = new proxy_protocol::NoCompression();
  }; break;
  case 2: {
    r = new proxy_protocol::SnappyCompression();
  }; break;
  case 3: {
    r = new proxy_protocol::BZip2Compression();
  }; break;
  default: { throw deserialisation_exception("unknown compression type"); };
  }
  p.reset(r);
}

void from(message &m, std::unique_ptr<proxy_protocol::EncryptInfo> &p) {
  uint8_t type;
  from(m, type);
  switch (type) {
  case 1: {
    p.reset(new proxy_protocol::NoEncryption());
  }; break;
  default: { throw deserialisation_exception("unknown encryption scheme)"); };
  }
}

template <> void from(message &m, proxy_protocol::Manifest &mf) {
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

template <>
void from(message &m, proxy_protocol::ManifestWithNamespaceId &mfid) {
  from(m, (proxy_protocol::Manifest &)mfid);
  from(m, mfid.namespace_id);
}

template <> void from(message &m, proxy_protocol::FCInfo &fc_info) {
  ALBA_LOG(DEBUG, "from(_, fc_info");
  from(m, fc_info.alba_id);
  from(m, fc_info.namespace_id);
  uint32_t n_chunks;
  from(m, n_chunks);
  ALBA_LOG(DEBUG, "n_chunks:" << n_chunks);

  for (uint32_t chunk_index = 0; chunk_index < n_chunks; ++chunk_index) {
    uint32_t chunk_id;
    from(m, chunk_id);
    ALBA_LOG(DEBUG, "chunk_id:" << chunk_id);
    int32_t n_fragments;
    from(m, n_fragments);
    std::map<int32_t, std::shared_ptr<proxy_protocol::Manifest>> chunk;
    ALBA_LOG(DEBUG, "n_fragments:" << n_fragments);
    for (int32_t fragment_index = 0; fragment_index < n_fragments;
         ++fragment_index) {
      int32_t fragment_id;
      from(m, fragment_id);
      ALBA_LOG(DEBUG, "fragment_id:" << fragment_id);

      std::shared_ptr<proxy_protocol::Manifest> mfp(
          new proxy_protocol::Manifest);
      from(m, *mfp);

      chunk[fragment_id] = std::move(mfp);
    }
    fc_info.info[chunk_id] = std::move(chunk);
  }
  ALBA_LOG(DEBUG, "m.pos= " << m.get_pos()
                            << " left=" << (m.size() - m.get_pos()));
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

void _dump_string(std::ostream &os, const std::string &s) {
  const char *bytes = s.data();
  const int size = s.size();
  stuff::dump_buffer(os, bytes, size);
}
std::ostream &operator<<(std::ostream &os, const Manifest &mf) {
  using alba::stuff::operator<<;
  os << "{"
     << "name = `";
  _dump_string(os, mf.name);
  os << "`, " << std::endl;
  os << "  object_id = `";
  _dump_string(os, mf.object_id);
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

std::ostream &operator<<(std::ostream &os, const FCInfo &fc_info) {
  os << "FCInfo{"
     << " alba_id = " << fc_info.alba_id
     << ", namespace_id = " << fc_info.namespace_id << ", info = ... }"
     << std::endl;
  return os;
}
}
}
