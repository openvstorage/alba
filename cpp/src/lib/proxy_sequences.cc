/*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
*/


#include "proxy_sequences.h"

namespace alba {
namespace llio {
template <>
void to(message_builder &mb,
        alba::proxy_client::sequences::Assert const &assert) noexcept {
  assert.to(mb);
}

template <>
void to(message_builder &mb,
        alba::proxy_client::sequences::Update const &update) noexcept {
  update.to(mb);
}
}
}
