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

#include "encryption.h"
#include "llio.h"

#include <gcrypt.h>

namespace alba {
namespace llio {

using namespace encryption;

template <> void from(message &m, chaining_mode_t &mode) {
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

template <> void from(message &m, key_length_t &kl) {
  uint8_t type;
  from(m, type);
  switch (type) {
  case 1: {
    kl = key_length_t::L256;
  } break;
  default: { throw deserialisation_exception("unknown key_length"); };
  }
}

template <> void from(message &m, algo_t &algo) {
  uint8_t type;
  from(m, type);
  switch (type) {
  case 1: {
    algo = algo_t::AES;
  }; break;
  default: { throw deserialisation_exception("unknown algo"); };
  }
}

template <> void from(message &m, Encrypted &awk) {

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

template <> void from(message &m, std::shared_ptr<EncryptInfo> &p) {
  uint8_t type;
  from(m, type);
  EncryptInfo *r;
  switch (type) {
  case 1: {
    r = new NoEncryption();
    p.reset(r);
  }; break;
  case 2: {
    Encrypted *awk = new Encrypted();
    r = awk;
    p.reset(r);
    from(m, *awk);
  }; break;
  default: {
    ALBA_LOG(WARNING, "unknown encryption scheme: type=" << type);
    throw deserialisation_exception("unknown encryption scheme)");
  };
  }
}
}
}

namespace alba {
namespace encryption {

bool Encrypted::partial_decrypt(unsigned char *buf, int len,
                                std::string &enc_key, std::string &ctr,
                                int offset) const {

  if (mode != chaining_mode_t::CTR) {
    return false;
  }

  uint64_t block_len = 16;

  if (ctr.size() != block_len) {
    assert(false);
  }

  uint64_t skip_blocks = offset / block_len;

  auto ctr_msg = llio::message(llio::message_buffer::from_string(ctr));

  uint64_t high, low;
  from_be(ctr_msg, high);
  from_be(ctr_msg, low);

  uint64_t low2 = low + skip_blocks;
  uint64_t high2 = high;
  if (std::numeric_limits<uint64_t>::max() - low < skip_blocks) {
    // low2 overflowed
    high2 += 1;
  }

  llio::message_builder mb;
  to_be(mb, high2);
  to_be(mb, low2);

  std::string ctr_with_offset = mb.as_string_no_size();

  int gcrypt_result;

  gcry_cipher_hd_t hd;
  gcrypt_result =
      gcry_cipher_open(&hd, GCRY_CIPHER_AES, GCRY_CIPHER_MODE_CTR, 0);
  if (gcrypt_result != 0) {
    ALBA_LOG(WARNING, "gcry_cipher_open returned " << gcrypt_result);
    return false;
  }

  gcrypt_result = gcry_cipher_setkey(hd, enc_key.c_str(), enc_key.size());
  if (gcrypt_result != 0) {
    ALBA_LOG(WARNING, "gcry_cipher_setkey returned " << gcrypt_result);
    gcry_cipher_close(hd);
    return false;
  }

  gcrypt_result = gcry_cipher_setctr(hd, ctr_with_offset.c_str(), block_len);
  if (gcrypt_result != 0) {
    ALBA_LOG(WARNING, "gcry_cipher_setctr returned " << gcrypt_result);
    gcry_cipher_close(hd);
    return false;
  }

  uint64_t to_burn = offset % block_len;
  if (to_burn > 0) {
    char burn_buf[to_burn];
    gcrypt_result = gcry_cipher_decrypt(hd, burn_buf, to_burn, nullptr, 0);
    if (gcrypt_result != 0) {
      ALBA_LOG(WARNING, "gcry_cipher_setctr returned " << gcrypt_result);
      gcry_cipher_close(hd);
      return false;
    }
  }

  gcrypt_result = gcry_cipher_decrypt(hd, buf, len, nullptr, 0);
  if (gcrypt_result != 0) {
    ALBA_LOG(WARNING, "gcry_cipher_setctr returned " << gcrypt_result);
    gcry_cipher_close(hd);
    return false;
  }

  gcry_cipher_close(hd);
  return true;
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
}
}
