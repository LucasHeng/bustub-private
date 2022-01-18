//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;
  // Remove the object that was accessed least recently compared to all the other elements being
  // tracked by the Replacer, store its contents in the output parameter and return True.
  // If the Replacer is empty return False.
  bool Victim(frame_id_t *frame_id) override;

  // This method should be called after a page is pinned to a frame in the BufferPoolManager.
  // It should remove the frame containing the pinned page from the LRUReplacer.
  void Pin(frame_id_t frame_id) override;

  // This method should be called when the pin_count of a page becomes 0.
  // This method should add the frame containing the unpinned page to the LRUReplacer.
  void Unpin(frame_id_t frame_id) override;

  // This method returns the number of frames that are currently in the LRUReplacer.
  size_t Size() override;

 private:
  // TODO(student): implement me!

  // latch
  std::mutex lru_mutex_;
  // free_list
  std::list<frame_id_t> lru_list_;
  // all frame
  std::vector<std::list<frame_id_t>::iterator> lru_vec_;
};

}  // namespace bustub
