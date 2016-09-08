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

#include "partial_read_planner.h"

namespace alba{
namespace proxy_protocol{

boost::optional<lookup_result_t>
lookup(const RoraMap &rora_map,
       int32_t offset) {

  ALBA_LOG(DEBUG, "_fragment_manifest(..., offset=" << offset);
  if (nullptr == rora_map.front){
      // only Manifest for back
      ManifestWithNamespaceId &back = *rora_map.back;
      auto result = back.to_chunk_fragment(offset);
      result -> _target = target_t::RORA_BACK;
      return result;
  } else{
      int32_t chunk_index;
      uint32_t chunk_endpoint;
      ManifestWithNamespaceId &back = *rora_map.back;
      // use back to get to the chunk & fragment.
      back.calculate_chunk_index(offset, chunk_index, chunk_endpoint);

      uint32_t chunk_size = back.chunk_sizes[chunk_index];
      uint32_t n_fragments = back.encoding_scheme.k;
      uint32_t fragment_length = chunk_size / n_fragments;
      uint32_t fragment_index = (offset - (chunk_size * chunk_index)) / fragment_length;

      auto &front = *rora_map.front;

      auto chunk = front.info.find(chunk_index)->second;
      ALBA_LOG(DEBUG, "chunk.size()=" << chunk.size());
      auto fragment = chunk.find(fragment_index);
      if (chunk.end() != fragment) {
          ALBA_LOG(DEBUG, "got entry:");
          auto mfp = fragment->second;
          using alba::stuff::operator<<;
          ALBA_LOG(DEBUG, "mfp=" << mfp);
          auto start_of_fragment =
              chunk_size * chunk_index + fragment_index * fragment_length;
          auto offset_in_fragment = offset - start_of_fragment;
          auto result = mfp->to_chunk_fragment(offset_in_fragment);
          if(boost::none != result){
              result -> _target = target_t::RORA_FRONT;
          }
          return result;
      } else {
          ALBA_LOG(DEBUG, "have no entry for chunk_index=" << chunk_index
                   << " fragment_index ="
                   << fragment_index);
          return boost::none;
      }
  }
}

int calculate_instructions_for_slice(const RoraMap &rora_map,
                                     const SliceDescriptor &sd,
                                     std::vector<std::shared_ptr<Instruction>>& instructions) {
  uint32_t bytes_to_read = sd.size;
  uint32_t offset = sd.offset;
  byte *buf = sd.buf;

  while (bytes_to_read > 0) {
    boost::optional<lookup_result_t> maybe_coords = lookup(rora_map, offset);
    uint32_t bytes_for_instruction;
    if (boost::none == maybe_coords) {
        auto via_proxy =
            std::shared_ptr<Instruction>(new ViaProxy(buf,
                                                      offset,
                                                      bytes_to_read));

        instructions.push_back(std::move(via_proxy));
        bytes_for_instruction = bytes_to_read;
    } else{
        auto &coords = *maybe_coords;
        ALBA_LOG(DEBUG, "coords=" << coords);
        if (coords.pos_in_fragment + bytes_to_read <= coords.fragment_length) {
            bytes_for_instruction = bytes_to_read;
        } else {
            bytes_for_instruction = coords.fragment_length - coords.pos_in_fragment;
        }
        string key;
        if(coords._target == target_t::RORA_BACK){
            key = "front:I can do this";
        } else{
            key = "back:???";
        }
        auto via_rora =
            std::shared_ptr<Instruction>(new ViaRora(
                                             key,
                                             coords._osd,
                                             buf,
                                             coords.pos_in_fragment,
                                             bytes_for_instruction,
                                             coords._target
                                             )
                );
        instructions.push_back(std::move(via_rora));
    }

    buf += bytes_for_instruction;
    offset += bytes_for_instruction;
    bytes_to_read -= bytes_for_instruction;
  }
  return 0;
}

void ViaProxy::pretty(std::ostream& os) const{
    os << "ViaProxy{" << "target= " << target
       << ", offset= " << _offset
       << ", size= "   << _size
       << "}";

}
void ViaRora::pretty(std::ostream& os) const{
    os << "ViaRora{"
       << "target= " << target
       << ", osd= "  << _osd
       << ", offset= " << _offset
       << ", size= "   << _size
       << "}";

    }

std::ostream& operator<<(std::ostream &os, const Instruction& i){
    i . pretty(os);
    return os;
}



}
}
