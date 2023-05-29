/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
// #include <asio/io_context.hpp>
// #include <asio/random_access_file.hpp>
// #include <asio/stream_file.hpp>
#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <fstream>

#include "coro_io/thread_pool.hpp"

// #include "asio/file_base.hpp"
// #include "asio_coro_util.hpp"
// #include "async_simple/coro/Lazy.h"

namespace ylt {

template <typename POOLTYPE = thread_pool>
class coro_file {
 public:
  using ThreadPoolPtr = std::shared_ptr<POOLTYPE>;
  coro_file(const std::string& filepath, ThreadPoolPtr tp, 
            size_t buf_size = 2048,
            std::ios_base::openmode mode = std::ios_base::in |
                                           std::ios_base::out)
      : filepath_(filepath), tp_(tp), buf_size_(buf_size) {
    file_.open(filepath, mode);
    if (!file_.is_open()) {
      std::cout << "failed to open file : [" << filepath_ << "]\n";
      return;
    }
    buf_.resize(buf_size_);
  }

  std::string_view filepath() {return filepath_; }

  std::future<std::string_view> async_read() {
    return tp_->enqueue_task([this]() {
      if (!file_.is_open()) {
        std::cout << "file [" << filepath_ <<  "] not open yet or has been closed\n";
        return std::string_view("");
      }
      file_.seekg(0, std::ios::end);
      auto filesize = file_.tellg();
      file_.seekg(0, std::ios::beg);
      buf_.resize(filesize);

      file_.read(&buf_[0], buf_.size());
      if (!file_.good()) {
          std::cout << "failed to read file: [" << filepath_ << "]\n";
          return std::string_view("");
      }
      auto size = file_.gcount(); 
      return std::string_view(buf_.data(), size);
    });
  }

  std::future<void> async_write(const char* data, size_t size) {
    // the user hold the lifetime of data 
    return tp_->enqueue_task([=]() {
      if (!file_.is_open()) {
        std::cout << "file [" << filepath_ <<  "] not open yet or has been closed\n";
        return;
      }
      file_.seekp(0, std::ios::end);
      file_.write(data, size);
      if (!file_) {
        std::cout << "failed to write file: [" << filepath_ << "]\n";
        return;
      }
      file_.flush();
    });
  }

  ~coro_file() = default;
 private:
  std::string filepath_;
  ThreadPoolPtr tp_;
  std::fstream file_;
  size_t buf_size_;
  std::vector<char> buf_;
};

} // namespace ylt
