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

LRUReplacer::LRUReplacer(size_t num_pages) {  // 我觉得页容量这个参数没啥用
  // capacity_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(lock_);
  if (data_.empty()) {
    return false;
  }

  *frame_id = data_.front();
  data_.pop_front();
  map_.erase(*frame_id);  // 同时删除map中该项
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(lock_);
  if (map_.count(frame_id) > 0) {
    data_.erase(map_[frame_id]);
    map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(lock_);
  // 我认为容量是没有用的，因为replacer的大小和pages大小一样，不可能超过
  if (map_.count(frame_id) == 0) {  // 之前不存在，对同一个元素调用两次unpin函数，第二次无效
    data_.emplace_back(frame_id);
    map_.insert({frame_id, --data_.end()});
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(lock_);
  return data_.size();
}

}  // namespace bustub
