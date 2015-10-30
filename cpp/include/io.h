/*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ALBA_IO_H
#define ALBA_IO_H

#include<iostream>
#include<vector>
#include<stdint.h>
#include<memory>
#include "llio.h"
#include "alba_logger.h"

namespace alba{


    template<typename T> void write_x(std::ostream& os, const T&t);

    template<typename T> void read_x(std::istream&, T&t);

    template<> void write_x<bool>(std::ostream& os, const bool& b);
    template<> void read_x<bool>(std::istream& is, bool& b);

    template<> void write_x<uint32_t>(std::ostream& os, const uint32_t& i);
    template<> void read_x<uint32_t>(std::istream& is, uint32_t& i);

    template<> void write_x<uint64_t>(std::ostream& os, const uint64_t& i);
    template<> void read_x<uint64_t>(std::istream& is, uint64_t& i);

    template<> void write_x<std::string>(std::ostream& os, const std::string& s);
    template<> void read_x<std::string>(std::istream& is, std::string& s);

    template<typename T>
        void write_x(std::ostream& os, const std::vector<T> & v){
        ALBA_LOG(DEBUG, __PRETTY_FUNCTION__);
        uint32_t size = v.size();
        write_x(os,size);
        for(auto iter = v.rbegin();iter!=v.rend();++iter){
            write_x(os,*iter);
        }
    }

    template<typename T>
        void read_vector(std::istream& is, std::vector<T> &v){
        uint32_t size;
        read_x<uint32_t>(is, size);
        v.resize(size);
        ALBA_LOG(DEBUG, "read_vector (size= " << size << ")")
        for(int32_t i = size -1; i >= 0; --i){
            T e;
            read_x<T>(is, e);
            v[i]=e;
        }
    }

    template<typename T>
        void write_x(std::ostream& os,
                     const std::shared_ptr<T>& xp
                     ){
        const T& x = * xp;
        write_x(os, x);
    }
}
#endif
