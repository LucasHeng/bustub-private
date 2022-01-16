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
//被锁定了不能置换，就不能放进list
//被启用了，那就从list删除

LRUReplacer::LRUReplacer(size_t num_pages) {
  // TO DO(student)
    lru_vec.resize(num_pages);
    for(int i=0;i<(int)num_pages;i++){
      lru_vec[i] = lru_list.end();
    }
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lru_mutex.lock();
  if (!lru_list.empty())
  {
    *frame_id = lru_list.back();
    lru_list.pop_back();
    lru_vec[*frame_id] = lru_list.end();
    lru_mutex.unlock();
    return true;
  }
  lru_mutex.unlock();
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lru_mutex.lock();
  if(lru_vec[frame_id]!=lru_list.end()){
    lru_list.erase(lru_vec[frame_id]);
    lru_vec[frame_id] = lru_list.end();
  }
  lru_mutex.unlock();


}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lru_mutex.lock();
  if(lru_vec[frame_id]==lru_list.end()){
    lru_list.push_front(frame_id);
    lru_vec[frame_id] = lru_list.begin();
  }
  lru_mutex.unlock();
}

size_t LRUReplacer::Size() {
  lru_mutex.lock();
  size_t size = lru_list.size();
  lru_mutex.unlock();
  return size;
}

}  // namespace bustub
