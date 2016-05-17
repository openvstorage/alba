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

#ifndef ALBA_CHECKSUM_H
#define ALBA_CHECKSUM_H

#include <iostream>
#include "io.h"
#include "stuff.h"
#include "llio.h"

namespace alba{
#define SHA_SIZE 20
    enum class algo_t { NO_CHECKSUM, SHA1, CRC32c };

    class Checksum {
    public:
        virtual ~Checksum() {};
        virtual algo_t get_algo() const = 0;
        virtual void print(std::ostream& os) const = 0;
        virtual void to(llio::message_builder& mb) const = 0;
    };

    class NoChecksum :public Checksum{
    public:
        virtual algo_t get_algo() const { return algo_t :: NO_CHECKSUM;}

        virtual void print(std::ostream& os) const {
            os << "NoChecksum()";
        }
        virtual void to(llio::message_builder&mb) const {
            mb.add_type(1);
        }
    };

    class Sha1 :public Checksum{
    public:
    Sha1(std::string& digest) : _digest(digest){};
        algo_t get_algo() const { return algo_t :: SHA1;};

        void to(llio::message_builder&mb) const{
            mb.add_type(2);
            llio::to<uint32_t>(mb, SHA_SIZE);
            mb.add_raw(_digest.data(), SHA_SIZE);
        }
        void print(std::ostream& os) const;

        std::string _digest;
    };

    class Crc32c :public Checksum{
    public:
    Crc32c(uint32_t digest) : _digest(digest){};
        algo_t get_algo() const { return algo_t :: CRC32c;};

        void to(llio::message_builder&mb) const{
            mb.add_type(3);
            llio::to<uint32_t>(mb, _digest);
        }
        void print(std::ostream& os) const;

        uint32_t _digest;
    };

    std:: ostream& operator<<(std::ostream&, const algo_t &);
    std:: ostream& operator<<(std::ostream&, const Checksum& );

    bool verify(const Checksum& c0, const Checksum& c1);
}

#endif
