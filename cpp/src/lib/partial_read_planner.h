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
#include "manifest.h"
#include "proxy_protocol.h"
#include "osd_access.h"

namespace alba{
namespace proxy_protocol{




    struct Instruction{
        target_t target;
        //offset,...
    Instruction(target_t t) : target(t){}
        virtual void pretty(std::ostream& os) const = 0;
        virtual ~Instruction(){};
        Instruction& operator=(const Instruction &) = delete;
        Instruction (const Instruction&) = delete;

    };

    int calculate_instructions_for_slice(const RoraMap &rora_map,
                                         const SliceDescriptor &sd,
                                         std::vector<std::shared_ptr<Instruction>> & instructions);
    struct ViaProxy : Instruction{

    ViaProxy(byte* buf,
             uint64_t offset,
             uint32_t size
        )
        : Instruction(target_t::VIA_PROXY)
        {
            _buf = buf;
            _offset = offset;
            size = size;
        }

        byte* _buf;
        uint64_t _offset;
        uint32_t _size;

        virtual void pretty(std::ostream& os) const;

    };

    struct ViaRora  : Instruction {
    ViaRora(std::string key,
            osd_t osd,
            byte* buf,
            uint64_t offset,
            uint32_t size,
            target_t target
        )
        : Instruction(target)
        {
            _osd = osd;
            _key = key;
            _buf = buf;
            _offset = offset;
            _size = size;
        }

        std::string _key;
        osd_t    _osd;
        byte*    _buf;
        uint64_t _offset;
        uint32_t _size;

        virtual void pretty(std::ostream& os) const;
    };


    boost::optional<lookup_result_t>
        lookup(const RoraMap &rora_map, int32_t offset);

    std::ostream& operator<<(std::ostream &os, const Instruction& i);
}

}
