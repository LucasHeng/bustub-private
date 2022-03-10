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
// 被锁定了不能置换，就不能放进list
// 被启用了，那就从list删除

LRUReplacer::LRUReplacer(size_t num_pages) {
  // TO DO(student)
  lru_vec_.resize(num_pages);
  for (int i = 0; i < static_cast<int>(num_pages); i++) {
    lru_vec_[i] = lru_list_.end();
  }
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lru_mutex_.lock();
  *frame_id = INVALID_PAGE_ID;
  if (!lru_list_.empty()) {
    *frame_id = lru_list_.back();
    lru_list_.pop_back();
    lru_vec_[*frame_id] = lru_list_.end();
    lru_mutex_.unlock();
    return true;
  }
  lru_mutex_.unlock();
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lru_mutex_.lock();
  if (lru_vec_[frame_id] != lru_list_.end()) {
    lru_list_.erase(lru_vec_[frame_id]);
    lru_vec_[frame_id] = lru_list_.end();
  }
  lru_mutex_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lru_mutex_.lock();
  if (lru_vec_[frame_id] == lru_list_.end()) {
    lru_list_.push_front(frame_id);
    lru_vec_[frame_id] = lru_list_.begin();
  }
  lru_mutex_.unlock();
}

size_t LRUReplacer::Size() {
  lru_mutex_.lock();
  size_t size = lru_list_.size();
  lru_mutex_.unlock();
  return size;
}

}  // namespace bustub
