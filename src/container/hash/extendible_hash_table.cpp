//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  Page *page = buffer_pool_manager_->NewPage(&directory_page_id_);
  HashTableDirectoryPage *hash_table_directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  hash_table_directory_page->PrintDirectory();
  hash_table_directory_page->SetPageId(directory_page_id_);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr);
  hash_table_directory_page->SetBucketPageId(0, bucket_page_id);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t directoryindex = Hash(key) & dir_page->GetGlobalDepthMask();
  return directoryindex;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t directoryindex = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(directoryindex);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  HashTableDirectoryPage *hash_table_directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return hash_table_directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  return hash_table_bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // first,find the directorypage
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  // second,find the page id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  // third,find the bucket_page
  Page *page_bucket = buffer_pool_manager_->FetchPage(bucket_page_id);
  page_bucket->WLatch();
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page_bucket->GetData());
  bool flag = hash_table_bucket_page->GetValue(key, comparator_, result);
  page_bucket->WUnlatch();
  table_latch_.WUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // LOG_DEBUG("Here is before Insert!!!");
  // VerifyIntegrity();
  // first,find the directorypage
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  // second,find the page id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  // third,find the bucket_page
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());

  bool flag = false;
  // if is full?
  if (hash_table_bucket_page->IsFull()) {
    table_latch_.WUnlock();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
    flag = SplitInsert(nullptr, key, value);
  } else {
    page->WLatch();
    flag = hash_table_bucket_page->Insert(key, value, comparator_);
    page->WUnlatch();
    table_latch_.WUnlock();
    if (flag) {
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
    } else {
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
    }
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  }
  // LOG_DEBUG("Here is after Insert!!!");
  // VerifyIntegrity();
  return flag;
}
//问题很大!!!
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // LOG_DEBUG("Here is before SplitInsert!!!");
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t directory_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_high_bit = dir_page->GetLocalHighBit(directory_index);

  HASH_TABLE_BUCKET_TYPE *bucket_table_page = FetchBucketPage(bucket_page_id);
  // new page for new bucket page
  uint32_t ld = dir_page->GetLocalDepth(directory_index);
  if (ld == dir_page->GetGlobalDepth()) {
    if ((1 << (dir_page->GetGlobalDepth() + 1)) <= DIRECTORY_ARRAY_SIZE) {
      dir_page->IncrGlobalDepth();
    } else {
      table_latch_.WUnlock();
      assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
      return false;
    }
  }
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id, nullptr);
  uint32_t diff = 0x1 << dir_page->GetLocalDepth(directory_index);
  HASH_TABLE_BUCKET_TYPE *new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());
  uint32_t current_size = dir_page->Size();
  // LOG_DEBUG("AAAXXXX,%d",new_page_id);
  for (uint32_t i = local_high_bit; i < current_size; i += diff) {
    // if the bucket_index is odd,put them in the new bucket_page
    // incresing local depth of the local bucket
    if ((i & diff) != 0) {
      dir_page->SetBucketPageId(i, new_page_id);
    } else {
      dir_page->SetBucketPageId(i, bucket_page_id);
    }
    // incresing local depth for the split_image_index;
    dir_page->IncrLocalDepth(i);
  }
  // dir_page->VerifyIntegrity();
  // LOG_DEBUG("AAAA");
  memcpy(reinterpret_cast<void *>(new_bucket_page), reinterpret_cast<void *>(bucket_table_page), PAGE_SIZE);
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (bucket_table_page->IsReadable(i)) {
      if (KeyToPageId(bucket_table_page->KeyAt(i), dir_page) == bucket_page_id) {
        new_bucket_page->RemoveAt(i);
      } else {
        bucket_table_page->RemoveAt(i);
      }
    }
  }
  table_latch_.WUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(new_page_id, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  // if is full?
  // if (bucket_table_page->IsFull()) {
  //   flag = SplitInsert(nullptr, key, value);
  // } else {
  //   flag = Insert(nullptr, key, value);
  // }
  // LOG_DEBUG("Here is after SplitInsert!!!");
  // VerifyIntegrity();
  return Insert(nullptr, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // LOG_TRACE("Here is before Remove!!!");
  // VerifyIntegrity();
  // first,find the directorypage
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  // second,find the page id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  // third,find the bucket_page
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool flag = hash_table_bucket_page->Remove(key, value, comparator_);
  page->WUnlatch();
  // bool is_merge = false;
  // uint32_t bucket_page_index = KeyToDirectoryIndex(key, dir_page);
  // uint32_t local_depth = dir_page->GetLocalDepth(bucket_page_index);
  // uint32_t split_image_index = dir_page->GetSplitImageIndex(KeyToDirectoryIndex(key, dir_page));
  // uint32_t split_local_depth = dir_page->GetLocalDepth(split_image_index);
  // if (hash_table_bucket_page->IsEmpty() && local_depth > 0 && local_depth == split_local_depth) {
  //   is_merge = true;
  // }
  // if (hash_table_bucket_page->IsEmpty()&&hash_table_bucket_page->GetLocalDepth()>0&&) {
  //   page->WUnlatch();
  //   table_latch_.RUnlock();
  //  Merge(transaction, key, value);
  // }
  table_latch_.WUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  if (flag) {
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  } else {
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
  }
  if (flag && hash_table_bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  // LOG_DEBUG("Here is after Remove!!!");
  // VerifyIntegrity();
  return flag;
}

/*****************************************************************************
 * MERGE
 * Merge should be called after remove and the bucket is empty.
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // LOG_DEBUG("Here is before Merge!!!");
  // VerifyIntegrity();
  // first,find the directorypage
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  // second,find the page id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_index);
  //
  uint32_t split_page_index = dir_page->GetSplitImageIndex(bucket_index);
  page_id_t split_page_id = dir_page->GetBucketPageId(split_page_index);
  uint32_t split_depth = dir_page->GetLocalDepth(split_page_index);
  if (split_page_id == bucket_page_id || local_depth <= 0 || local_depth != split_depth) {
    table_latch_.WUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
    return;
  }
  // third,find the bucket_page
  // HASH_TABLE_BUCKET_TYPE *split_table_bucket_page = FetchBucketPage(split_page_id);
  // HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);

  uint32_t new_ld = 0x1 << (dir_page->GetLocalDepth(bucket_index) - 1);
  uint32_t local_mask = bucket_index % new_ld;
  uint32_t current_size = dir_page->Size();
  for (uint32_t i = local_mask; i < current_size; i += new_ld) {
    dir_page->SetBucketPageId(i, split_page_id);
    dir_page->DecrLocalDepth(i);
  }
  // dir_page->VerifyIntegrity();
  buffer_pool_manager_->DeletePage(bucket_page_id, nullptr);
  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  // assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  // assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  table_latch_.WUnlock();
  // if (split_table_bucket_page->IsEmpty()) {
  //   Merge(transaction, key, value);
  // }
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  // assert(buffer_pool_manager_->UnpinPage(split_page_id, true, nullptr));
  // LOG_DEBUG("Here is after Merge!!!");
  // VerifyIntegrity();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}


template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintDir() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t dir_size = dir_page->Size();

  dir_page->PrintDirectory();
  printf("dir size is: %d\n", dir_size);
  for (uint32_t idx = 0; idx < dir_size; idx++) {
    auto bucket_page_id = dir_page->GetBucketPageId(idx);
    HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
    bucket_page->PrintBucket();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  }

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}


template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::RemoveAllItem(Transaction *transaction, uint32_t bucket_idx) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  auto items = bucket_page->GetAllItem();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  table_latch_.RUnlock();
  for (auto &item : items) {
    Remove(nullptr, item.first, item.second);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
}




/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
