// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/slice.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "util/memcmp.h"

namespace starrocks {

class faststring;

/// @brief A wrapper around externally allocated data.
///
/// Slice is a simple structure containing a pointer into some external
/// storage and a size. The user of a Slice must ensure that the slice
/// is not used after the corresponding external storage has been
/// deallocated.
///
/// Multiple threads can invoke const methods on a Slice without
/// external synchronization, but if any of the threads may call a
/// non-const method, all threads accessing the same Slice must use
/// external synchronization.
class Slice {
public:
    char* data;
    size_t size{0};

    static void init();
    static const Slice& max_value();
    static const Slice& min_value();

    // Intentionally copyable

    /// Create an empty slice.
    Slice() : data(const_cast<char*>("")) {}

    /// Create a slice that refers to a @c char byte array.
    Slice(const char* d, size_t n) : data(const_cast<char*>(d)), size(n) {}

    // Create a slice that refers to a @c uint8_t byte array.
    //
    // @param [in] d
    //   The input array.
    // @param [in] n
    //   Number of bytes in the array.
    Slice(const uint8_t* s, size_t n) : data(const_cast<char*>(reinterpret_cast<const char*>(s))), size(n) {}

    /// Create a slice that refers to the contents of the given string.
    Slice(const std::string& s)
            : // NOLINT(runtime/explicit)
              data(const_cast<char*>(s.data())),
              size(s.size()) {}

    Slice(std::string_view s) : data(const_cast<char*>(s.data())), size(s.size()) {}

    Slice(const faststring& s);

    /// Create a slice that refers to a C-string s[0,strlen(s)-1].
    Slice(const char* s)
            : // NOLINT(runtime/explicit)
              data(const_cast<char*>(s)),
              size(strlen(s)) {}

    operator std::string_view() const { return {data, size}; }

    /// @return A pointer to the beginning of the referenced data.
    const char* get_data() const { return data; }

    /// @return A mutable pointer to the beginning of the referenced data.
    char* mutable_data() { return const_cast<char*>(data); }

    /// @return The length (in bytes) of the referenced data.
    size_t get_size() const { return size; }

    /// @return @c true iff the length of the referenced data is zero.
    bool empty() const { return size == 0; }

    /// @return the n-th byte in the referenced data.
    const char& operator[](size_t n) const {
        assert(n < size);
        return data[n];
    }

    /// Change this slice to refer to an empty array.
    void clear() {
        data = const_cast<char*>("");
        size = 0;
    }

    /// Drop the first "n" bytes from this slice.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   Number of bytes that should be dropped from the beginning.
    void remove_prefix(size_t n) {
        assert(n <= size);
        data += n;
        size -= n;
    }

    /// Drop the last "n" bytes from this slice.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   Number of bytes that should be dropped from the tail.
    void remove_suffix(size_t n) {
        assert(n <= size);
        size -= n;
    }

    /// Truncate the slice to the given number of bytes.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   The new size of the slice.
    void truncate(size_t n) {
        assert(n <= size);
        size = n;
    }

    /// @return A string that contains a copy of the referenced data.
    std::string to_string() const { return std::string(data, size); }

    /// Do a three-way comparison of the slice's data.
    int compare(const Slice& b) const;

    /// Check whether the slice starts with the given prefix.
    bool starts_with(const Slice& x) const { return ((size >= x.size) && (memequal(data, x.size, x.data, x.size))); }

    bool ends_with(const Slice& x) const {
        return ((size >= x.size) && memequal(data + (size - x.size), x.size, x.data, x.size));
    }

    Slice tolower(std::string& buf) {
        // copy this slice into buf
        buf.assign(get_data(), get_size());
        std::transform(buf.begin(), buf.end(), buf.begin(), [](unsigned char c) { return std::tolower(c); });
        return Slice(buf.data(), buf.size());
    }

    /// @brief Comparator struct, useful for ordered collections (like STL maps).
    struct Comparator {
        /// Compare two slices using Slice::compare()
        ///
        /// @param [in] a
        ///   The slice to call Slice::compare() at.
        /// @param [in] b
        ///   The slice to use as a parameter for Slice::compare().
        /// @return @c true iff @c a is less than @c b by Slice::compare().
        bool operator()(const Slice& a, const Slice& b) const { return a.compare(b) < 0; }
    };

    /// Relocate/copy the slice's data into a new location.
    ///
    /// @param [in] d
    ///   The new location for the data. If it's the same location, then no
    ///   relocation is done. It is assumed that the new location is
    ///   large enough to fit the data.
    void relocate(char* d) {
        if (data != d) {
            memcpy(d, data, size);
            data = d;
        }
    }

    friend bool operator==(const Slice& x, const Slice& y);

    friend std::ostream& operator<<(std::ostream& os, const Slice& slice);

    static size_t compute_total_size(const std::vector<Slice>& slices) {
        size_t total_size = 0;
        for (auto& slice : slices) {
            total_size += slice.size;
        }
        return total_size;
    }

    static std::string to_string(const std::vector<Slice>& slices) {
        std::string buf;
        for (auto& slice : slices) {
            buf.append(slice.data, slice.size);
        }
        return buf;
    }
};

inline std::ostream& operator<<(std::ostream& os, const Slice& slice) {
    os << slice.to_string();
    return os;
}

/// Check whether two slices are identical.
inline bool operator==(const Slice& x, const Slice& y) {
    return memequal(x.data, x.size, y.data, y.size);
}

/// Check whether two slices are not identical.
inline bool operator!=(const Slice& x, const Slice& y) {
    return !(x == y);
}

inline int Slice::compare(const Slice& b) const {
    return memcompare(data, size, b.data, b.size);
}

inline bool operator<(const Slice& lhs, const Slice& rhs) {
    return lhs.compare(rhs) < 0;
}

inline bool operator<=(const Slice& lhs, const Slice& rhs) {
    return lhs.compare(rhs) <= 0;
}

inline bool operator>(const Slice& lhs, const Slice& rhs) {
    return lhs.compare(rhs) > 0;
}

inline bool operator>=(const Slice& lhs, const Slice& rhs) {
    return lhs.compare(rhs) >= 0;
}

/// @brief STL map whose keys are Slices.
///
/// An example of usage:
/// @code
///   typedef SliceMap<int>::type MySliceMap;
///
///   MySliceMap my_map;
///   my_map.insert(MySliceMap::value_type(a, 1));
///   my_map.insert(MySliceMap::value_type(b, 2));
///   my_map.insert(MySliceMap::value_type(c, 3));
///
///   for (const MySliceMap::value_type& pair : my_map) {
///     ...
///   }
/// @endcode
template <typename T>
struct SliceMap {
    /// A handy typedef for the slice map with appropriate comparison operator.
    typedef std::map<Slice, T, Slice::Comparator> type;
};

// A move-only type which manage the lifecycle of externally allocated data.
// Unlike std::unique_ptr<uint8_t[]>, OwnedSlice remembers the size of data so that clients can access
// the underlying buffer as a Slice.
//
// Usage example:
//   OwnedSlice read_page(PagePointer pp);
//   {
//     OwnedSlice page_data(new uint8_t[pp.size], pp.size);
//     Status s = _file.read_at(pp.offset, owned.slice());
//     if (!s.ok()) {
//       return s; // `page_data` destructs, deallocate underlying buffer
//     }
//     return page_data; // transfer ownership of buffer into the caller
//   }
//
class OwnedSlice {
public:
    OwnedSlice() : _slice((uint8_t*)nullptr, 0) {}

    OwnedSlice(uint8_t* _data, size_t size) : _slice(_data, size) {}

    // disable copy constructor and copy assignment
    OwnedSlice(const OwnedSlice&) = delete;
    void operator=(const OwnedSlice&) = delete;

    OwnedSlice(OwnedSlice&& src) noexcept : _slice(src._slice) {
        src._slice.data = nullptr;
        src._slice.size = 0;
    }

    OwnedSlice& operator=(OwnedSlice&& src) noexcept {
        OwnedSlice tmp(std::move(src));
        this->swap(tmp);
        return *this;
    }

    ~OwnedSlice() { delete[] _slice.data; }

    const Slice& slice() const { return _slice; }

    void swap(OwnedSlice& rhs) {
        using std::swap;
        swap(_slice, rhs._slice);
    }

    bool is_loaded() { return _slice.get_data() && (_slice.get_size() > 0); }

private:
    friend void swap(OwnedSlice& s1, OwnedSlice& s2);

    Slice _slice;
};

inline void swap(OwnedSlice& s1, OwnedSlice& s2) {
    s1.swap(s2);
}

} // namespace starrocks
