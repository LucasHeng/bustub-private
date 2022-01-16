//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances) {
  // Allocate and create individual BufferPoolManagerInstances
  start_index_ = 0;
  for (int i = 0; i != static_cast<int>(num_instances_); i++) {
    bpis_.emplace_back(new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return bpis_.size();
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  page_id_t target_id = page_id % num_instances_;
  return bpis_[target_id];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  Page *p = bpm->FetchPage(page_id);
  return p;
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  bool flag = bpm->UnpinPage(page_id, is_dirty);
  return flag;
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  bool flag = bpm->FlushPage(page_id);
  return flag;
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  int start = start_index_;
  Page *p = nullptr;
  do {
    BufferPoolManager *bpm = GetBufferPoolManager(static_cast<page_id_t>(start));
    p = bpm->NewPage(page_id);
    if (p != nullptr) {
      break;
    }
    start = static_cast<int>((start + 1) % num_instances_);
    /* code */
  } while (start != start_index_);

  return p;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  bool flag = bpm->DeletePage(page_id);
  return flag;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (int i = 0; i != static_cast<int>(num_instances_); i++) {
    BufferPoolManager *bpm = bpis_[i];
    bpm->FlushAllPages();
  }
}

}  // namespace bustub
