#include <cassert>
#include <fstream>
#include <memory>
#include <string_view>
#include <thread>

#include "coro_io/thread_pool.hpp"
#include "coro_io/coro_file.hpp"

void create_temp_file(std::string filename, size_t size) {
  std::ofstream file(filename, std::ios::binary);
  file.exceptions(std::ios_base::failbit | std::ios_base::badbit);

  {
    std::string str(size, 'a');
    file.write(str.data(), str.size());
  }
  {
    std::string str(size, 'b');
    file.write(str.data(), str.size());
  }
  {
    std::string str(42, 'c');
    file.write(str.data(), str.size());
  }

  file.flush();  // can throw
}

void test_write_and_read_file() {
  std::string filename = "/tmp/test.txt";
  create_temp_file(filename, 0);

  auto tp = std::make_shared<ylt::thread_pool>(1);
  ylt::coro_file file(filename, tp);
  std::string str(100, 'a');
  auto future_write = file.async_write(str.data(), str.size());
  future_write.get();
  auto future_read = file.async_read();
  std::cout<< "file content : \n" << future_read.get() << std::endl;
}

void test_thread_pool() {
  auto tp = std::make_shared<ylt::thread_pool>(2);
  std::thread::id id1, id2, id3;
  std::vector<std::future<void>> futures(4);
  std::atomic<bool> done[4] = {false, false, false, false};
  std::function<void()> f1 = [tp, &done, &id1]() {
      id1 = std::this_thread::get_id();
      assert(tp->getCurrentId() == 0);
      done[0] = true;
  };
  std::function<void()> f2 = [tp, &done, &id2]() {
      id2 = std::this_thread::get_id();
      assert(tp->getCurrentId() == 0);
      done[1] = true;
  };
  std::function<void()> f3 = [tp, &done, &id3]() {
      id3 = std::this_thread::get_id();
      assert(tp->getCurrentId() == 1);
      done[2] = true;
  };
  std::function<void()> f4 = [&done]() { done[3] = true; };
  futures[0] = tp->enqueue_task(std::move(f1), 0);
  futures[1] = tp->enqueue_task(std::move(f2), 0);
  futures[2] = tp->enqueue_task(std::move(f3), 1);
  futures[3] = tp->enqueue_task(std::move(f4));
  for (int i = 0; i < 4; ++i) {
    futures[i].get();
    assert(done[i]);
  }
  std::cout << "thread id1 : " << id1 << std::endl;
  std::cout << "thread id3 : " << id3 << std::endl;
  assert(id1 == id2);
  assert(id1 != id3);
  assert(tp->getCurrentId() == -1);
}

int main() {
  test_thread_pool();
  test_write_and_read_file();
}
