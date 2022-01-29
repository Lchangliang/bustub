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

LRUReplacer::LRUReplacer(size_t num_pages) : max_num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (unpin_list_.empty()) {
    return false;
  }
  frame_id_t tmp = unpin_list_.back();
  unpin_list_.pop_back();
  unpin_map_.erase(tmp);
  if (frame_id != nullptr) {
    *frame_id = tmp;
  }
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = unpin_map_.find(frame_id);
  if (iter == unpin_map_.end()) {
    return;
  }
  unpin_list_.erase(iter->second);
  unpin_map_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = unpin_map_.find(frame_id);
  if (iter == unpin_map_.end()) {
    BUSTUB_ASSERT(unpin_list_.size() < max_num_pages_, "LRUReplacer should not be full");
    unpin_list_.push_front(frame_id);
    unpin_map_[frame_id] = unpin_list_.begin();
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch_);
  return unpin_list_.size();
}

}  // namespace bustub
