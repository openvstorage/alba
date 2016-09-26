/*
  Copyright (C) 2016 iNuron NV

  This file is part of Open vStorage Open Source Edition (OSE), as available
  from


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

#include "checksum.h"
#include "llio.h"
#include <memory>

namespace alba {
namespace proxy_client {
namespace sequences {

class Assert {
public:
  virtual ~Assert(){};
  virtual void to(llio::message_builder &) const = 0;
};

class AssertObjectExists : Assert {
public:
  AssertObjectExists(std::string name) : _name(name){};

  void to(llio::message_builder &mb) const {
    mb.add_type(1);
    llio::to(mb, _name);
  }

  std::string _name;
};

class AssertObjectDoesNotExist : Assert {
public:
  AssertObjectDoesNotExist(std::string name) : _name(name){};

  void to(llio::message_builder &mb) const {
    mb.add_type(2);
    llio::to(mb, _name);
  }

  std::string _name;
};

class AssertObjectHasId : Assert {
public:
  AssertObjectHasId(std::string name, std::string object_id)
      : _name(name), _object_id(object_id){};

  void to(llio::message_builder &mb) const {
    mb.add_type(3);
    llio::to(mb, _name);
    llio::to(mb, _object_id);
  }

  std::string _name;
  std::string _object_id;
};

class AssertObjectHasChecksum : Assert {
public:
  AssertObjectHasChecksum(std::string name, std::unique_ptr<alba::Checksum> cs)
      : _name(name), _cs(std::move(cs)){};

  void to(llio::message_builder &mb) const {
    mb.add_type(4);
    llio::to(mb, _name);
    _cs->to(mb);
  }

  std::string _name;
  std::unique_ptr<alba::Checksum> _cs;
};

class Update {
public:
  virtual ~Update(){};
  virtual void to(llio::message_builder &) const = 0;
};

class UpdateUploadObjectFromFile : Update {
public:
  UpdateUploadObjectFromFile(std::string name, std::string file_name,
                             alba::Checksum *cs_o)
      : _name(name), _file_name(file_name), _cs_o(cs_o){};
  void to(llio::message_builder &mb) const {
    mb.add_type(1);
    llio::to(mb, _name);
    llio::to(mb, _file_name);
    if (_cs_o == nullptr) {
      alba::llio::to<boost::optional<int>>(mb, boost::none);
    } else {
      llio::to(mb, boost::optional<const Checksum *>(_cs_o));
    }
  }

  std::string _name;
  std::string _file_name;
  alba::Checksum *_cs_o;
};

/* TODO upload object from in memory blob */

class UpdateDeleteObject : Update {
public:
  UpdateDeleteObject(std::string name) : _name(name){};
  void to(llio::message_builder &mb) const {
    mb.add_type(3);
    llio::to(mb, _name);
  }

  std::string _name;
};
}
}
}
