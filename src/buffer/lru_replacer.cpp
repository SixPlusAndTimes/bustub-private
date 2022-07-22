//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {
// 容量这个属性好像没有用到阿
LRUReplacer::LRUReplacer(size_t num_pages):capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;
// Remove the object that was accessed least recently compared to all the other elements being tracked by the Replacer,
// store its contents in the output parameter and return True. If the Replacer is empty return False.
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // latch_.lock();
  if (list_.empty()) {
    // latch_.unlock();
    return false;
  }
  *frame_id = list_.back();
  map_.erase(*frame_id);
  list_.pop_back();
  // latch_.unlock();
  return true;
}

// This method should be called after a page is pinned to a frame in the BufferPoolManager.
// It should remove the frame containing the pinned page from the LRUReplacer.
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // latch_.lock();
  if (map_.count(frame_id) == 0) {
    // latch_.unlock();
    return;
  }
  // std::list<frame_id_t>::iterator to_remove = map_[frame_id];
  map_.erase(frame_id);
  list_.erase(map_[frame_id]);
  // latch_.unlock();
}

// This method should be called when the pin_count of a page becomes 0. This method should add the frame containing the
// unpinned page to the LRUReplacer. 当一个页没有被pin住时才会被加入lru_repalcer中，表示可以将它从内存中驱逐 unpin :
// 如果这个页已经存在了，不需要将该页移动到前端！！什么都别作
// 本来还以为要加timestamp啥的呢
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // latch_.lock();
  if (map_.count(frame_id) != 0) {
    // lru_replacer中已经有了这个page
    // latch_.unlock();
    return;
  }
  list_.push_front(frame_id);
  //  该写法错误 map_.insert(frame_id, list_.front()); 参考cppreference ，insert
  //  的参数应该是一个std::pair才可以。我靠，vscode竟然不报错！
  map_[frame_id] = list_.begin();
  // latch_.unlock();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(latch_);
  return list_.size();
}

}  // namespace bustub
