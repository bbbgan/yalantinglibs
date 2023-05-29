#pragma once

#include <queue>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <functional>
#include <future>
#include <memory>
#include <iostream>
#include <async_simple/util/Queue.h>

namespace ylt {

namespace detail {
  
// Whether or not do work steal for fn.
// If the user don't assign a thread to fn,
// thread pool will apply random policy to fn.
// If enable canSteal,
// thread pool will apply work steal policy firstly , if failed, will
// apply random policy to fn.
class task_wrapper {
 public:
  template <typename F>
  task_wrapper(F &&f, bool can_steal)
      : impl_(new impl_type<F>(std::move(f))), can_steal_(can_steal) {}

  void operator()() { impl_->call(); }

  task_wrapper(task_wrapper &&other)
      : impl_(std::move(other.impl_)), can_steal_(other.can_steal_) {}

  task_wrapper &operator=(task_wrapper &&other) {
    impl_ = std::move(other.impl_);
    can_steal_ = other.can_steal_;
    return *this;
  }

  void set_steal(bool can_steal) {can_steal_ = can_steal; }
  bool can_steal() { return can_steal_; }

  task_wrapper() = default;
  task_wrapper(const task_wrapper &) = delete;
  task_wrapper(task_wrapper &) = delete;
  task_wrapper &operator=(task_wrapper &) = delete;

 private:
  struct impl_base {
    virtual void call() = 0;
    virtual ~impl_base() {}
  };

  template <typename F>
  struct impl_type : impl_base {
    impl_type(F&& f) : f_(std::move(f)) {}
    void call() override{
      if (f_.valid()) f_();
    }
    F f_;
  };
  std::unique_ptr<impl_base> impl_;  
  bool can_steal_;
};

} // namespace detail

class thread_pool {
 public:
  using wrapper = detail::task_wrapper;
  template<typename T>
  using task_queue = async_simple::util::Queue<T>;

  explicit thread_pool(size_t threadNum = std::thread::hardware_concurrency(),
                      bool enableWorkSteal = false,
                      bool enableCoreBindings = false);
  ~thread_pool();
  thread_pool(const thread_pool&) = delete;
  thread_pool& operator=(const thread_pool&) = delete;

  template <typename F>
  inline std::future<typename std::result_of<F()>::type> 
  enqueue_task(F &&fn, int32_t id = -1);
  

  int32_t getCurrentId() const;
  size_t getItemCount() const;
  int32_t getThreadNum() const { return _threadNum; }

 private:

  std::pair<size_t, thread_pool *> *getCurrent() const;
  int32_t _threadNum;

  std::vector<task_queue<wrapper>> _queues;
  std::vector<std::thread> _threads;

  std::atomic<bool> _stop;
  bool _enableWorkSteal;
  bool _enableCoreBindings;
};

#ifdef __linux__
static void getCurrentCpus(std::vector<uint32_t> &ids) {
  cpu_set_t set;
  ids.clear();
  if (sched_getaffinity(0, sizeof(set), &set) == 0)
    for (uint32_t i = 0; i < CPU_SETSIZE; i++)
      if (CPU_ISSET(i, &set)) ids.emplace_back(i);
}
#endif

inline thread_pool::thread_pool(size_t threadNum, bool enableWorkSteal,
                              bool enableCoreBindings)
    : _threadNum(threadNum),
      _queues(_threadNum),
      _stop(false),
      _enableWorkSteal(enableWorkSteal),
      _enableCoreBindings(enableCoreBindings) {
  auto worker = [this](size_t id) {
    auto current = getCurrent();
    current->first = id;
    current->second = this;
    while (!_stop) {
      wrapper item;
      bool stole = false;
      if (_enableWorkSteal) {
        // Try to do work steal firstly.
        for (auto n = 0; n < _threadNum * 2; ++n) {
          if (_queues[(id + n) % _threadNum].try_pop_if(
                  item, [](auto &it) { return it.can_steal(); })) {
            stole = true;
            break;
          }
        }
      }
      // If _enableWorkSteal false or work steal failed, wait for a pop
      // task.
      if (!stole && !_queues[id].pop(item)) continue;
      item();
    }
  };

  _threads.reserve(_threadNum);

#ifdef __linux__
  // Since the CPU IDs might not start at 0 and might not be continuous
  // in the containers,
  // we need to get the available cpus at first.
  std::vector<uint32_t> cpu_ids;
  if (_enableCoreBindings) getCurrentCpus(cpu_ids);
#else
  // Avoid unused member warning.
  // [[maybe_unused]] in non-static data members is ignored in GCC.
  (void)_enableCoreBindings;
#endif

  for (auto i = 0; i < _threadNum; ++i) {
    _threads.emplace_back(worker, i);

#ifdef __linux__
    if (!_enableCoreBindings) continue;

    // Run threads per core.
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_ids[i % cpu_ids.size()], &cpuset);
    int rc = sched_setaffinity(_threads[i].native_handle(), sizeof(cpu_set_t),
                               &cpuset);
    if (rc != 0) std::cerr << "Error calling sched_setaffinity: " << rc << "\n";
#endif
  }
}

inline thread_pool::~thread_pool() {
  _stop = true;
  for (auto &queue : _queues) queue.stop();
  for (auto &thread : _threads) thread.join();
}

template <typename F>
inline std::future<typename std::result_of<F()>::type> 
thread_pool::enqueue_task(F &&fn, int32_t id) {
  using T = typename std::result_of<F()>::type;
  // if (nullptr == fn || _stop) {
  if (_stop) {
    std::cout << "stop or nullptr" << std::endl;
    return {};
  }
  std::packaged_task<T()> task(std::move(fn));
  std::future<T> res(task.get_future());
  if (id == -1) {
    if (_enableWorkSteal) {
      // Try to push to a non-block queue firstly.
      wrapper item(std::move(task), true);
      for (auto n = 0; n < _threadNum * 2; ++n) {
        if (_queues.at(n % _threadNum).try_push(std::move(item))) return res;
      }
    }

    id = rand() % _threadNum;
    _queues[id].push(wrapper{std::move(task), _enableWorkSteal});
  } else {
    assert(id < _threadNum);
     _queues[id].push(wrapper{std::move(task), false});
  }
  return res;
}

inline std::pair<size_t, thread_pool *> *thread_pool::getCurrent() const {
  static thread_local std::pair<size_t, thread_pool *> current(-1, nullptr);
  return &current;
}

inline int32_t thread_pool::getCurrentId() const {
  auto current = getCurrent();
  if (this == current->second) {
    return current->first;
  }
  return -1;
}

inline size_t thread_pool::getItemCount() const {
  size_t ret = 0;
  for (auto i = 0; i < _threadNum; ++i) {
    ret += _queues[i].size();
  }
  return ret;
}
} // namespace ylt