//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

#include "common/logger.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // 刷新进磁盘，数据不dirty了
  ValidatePageId(page_id);
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    Page *p = &pages_[page_table_[page_id]];
    disk_manager_->WritePage(page_id, p->data_);
    p->is_dirty_ = false;
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (auto page : page_table_) {
    page_id_t page_id = page.first;
    FlushPgImp(page_id);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  Page *p = nullptr;
  // pick a page from the free list first
  if (!free_list_.empty()) {
    // next page id in the bpi
    *page_id = next_page_id_;
    next_page_id_ += num_instances_;
    ValidatePageId(*page_id);

    frame_id_t frame_id = free_list_.front();  // find a frame for the page
    free_list_.pop_front();
    page_table_[*page_id] = frame_id;
    replacer_->Pin(frame_id);
    p = &pages_[frame_id];
    p->ResetMemory();
    // disk_manager_->ReadPage(*page_id, p->data_);

    p->page_id_ = *page_id;
    p->is_dirty_ = false;
    p->pin_count_ = 1;
  } else {
    frame_id_t r;
    // find a frame from replacer
    if (replacer_->Victim(&r)) {
      *page_id = next_page_id_;
      next_page_id_ += num_instances_;

      p = &pages_[r];
      if (p->IsDirty()) {
        disk_manager_->WritePage(p->page_id_, p->data_);
        p->is_dirty_ = false;
      }
      page_table_[*page_id] = r;
      p->ResetMemory();
      if (p->page_id_ != INVALID_PAGE_ID) {
        page_table_.erase(p->page_id_);
      }
      // disk_manager_->ReadPage(*page_id, p->data_);
      disk_manager_->WritePage(*page_id, p->data_);
      p->page_id_ = *page_id;
      p->is_dirty_ = false;
      p->pin_count_ = 1;
      replacer_->Pin(r);
    }
  }
  latch_.unlock();
  // LOG_DEBUG("HERE:%d\n",*page_id);
  return p;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  ValidatePageId(page_id);
  Page *p = nullptr;
  latch_.lock();
  // if the page p in buffer pool,return it and pin it
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    p = &pages_[frame_id];
    p->pin_count_++;

    if (p->pin_count_ == 1) {
      replacer_->Pin(frame_id);
    }
  } else {
    // if p not in buffer,find it from disk
    if (!free_list_.empty()) {
      // find a replacement page from the free lists
      frame_id_t r = free_list_.front();
      free_list_.pop_front();
      page_table_[page_id] = r;
      p = &pages_[r];
      if (p->IsDirty()) {
        disk_manager_->WritePage(p->GetPageId(), p->GetData());
        p->is_dirty_ = false;
      }
      p->ResetMemory();
      p->page_id_ = page_id;
      p->pin_count_ = 1;
      replacer_->Pin(r);
      disk_manager_->ReadPage(page_id, p->data_);
    } else {
      // find a replacement page from replacer,there is no page in free lists
      frame_id_t r;
      if (replacer_->Victim(&r)) {
        page_table_[page_id] = r;
        p = &pages_[r];
        page_table_.erase(p->page_id_);
        if (p->IsDirty()) {
          disk_manager_->WritePage(p->page_id_, p->data_);
          p->is_dirty_ = false;
        }
        p->ResetMemory();
        p->page_id_ = page_id;
        p->pin_count_ = 1;
        replacer_->Pin(r);
        disk_manager_->ReadPage(page_id, p->data_);
      }
    }
  }
  latch_.unlock();
  return p;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  ValidatePageId(page_id);
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    DeallocatePage(page_id);
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *p = &pages_[frame_id];
  if (p->GetPinCount() != 0) {
    latch_.unlock();
    return false;
  }

  DeallocatePage(page_id);
  replacer_->Unpin(frame_id);
  page_table_.erase(page_id);
  p->ResetMemory();
  p->is_dirty_ = false;
  p->pin_count_ = 0;
  p->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  latch_.unlock();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // this page_id should be in this bfpi
  ValidatePageId(page_id);
  latch_.lock();
  // page not in buffer
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  // page in buffer
  frame_id_t r = page_table_[page_id];
  Page *p = &pages_[r];
  // if (p->pin_count_ > 1) {
  //   printf("pin_count:%d\n",p->pin_count_);
  // }
  if (is_dirty) {  // page is dirty
    p->is_dirty_ = is_dirty;
  }
  if (p->pin_count_ > 0) {
    p->pin_count_--;
    if (p->pin_count_ == 0) {
      replacer_->Unpin(r);
    }
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

size_t BufferPoolManagerInstance::GetOccupiedPageNum() { 
  LOG_DEBUG("1:%ld\t2:%ld\n",page_table_.size(),replacer_->Size());
  return page_table_.size() - replacer_->Size(); 
}

void BufferPoolManagerInstance::PrintExistPageId() {
  for (auto item : page_table_) {
    printf("page id is:%d pin count is %d\n", item.first, pages_[item.second].pin_count_);
  }
}

}  // namespace bustub
