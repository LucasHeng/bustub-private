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
  //
  directory_page_id_ = dir_page->GetPageId();

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
  // second,find the page id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  // third,find the bucket_page
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);
  return hash_table_bucket_page->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  // first,find the directorypage
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  // second,find the page id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  // third,find the bucket_page
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);

  bool flag = false;
  // if is full?
  if (hash_table_bucket_page->IsFull()) {
    table_latch_.RUnlock();
    flag = SplitInsert(nullptr, key, value);
  } else {
    flag = hash_table_bucket_page->Insert(key, value, comparator_);
    table_latch_.RUnlock();
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t directory_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_high_bit = dir_page->GetLocalHighBit(directory_index);

  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);
  bool flag = false;
  // new page for new bucket page
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id, nullptr);
  HASH_TABLE_BUCKET_TYPE *new_hash_table_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());
  uint32_t ld = dir_page->GetLocalDepth(directory_index);
  if (ld == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }
  uint32_t gd = dir_page->GetGlobalDepth();
  for (uint32_t i = 0; i < static_cast<uint32_t>(1 << (gd - ld)); i++) {
    // if the bucket_index is odd,put them in the new bucket_page
    // incresing local depth of the local bucket
    uint32_t split_image_index = static_cast<uint32_t>(1 << ld) * i + local_high_bit;
    if (i % 2 == 1) {
      dir_page->SetBucketPageId(split_image_index, new_page_id);
    }
    // incresing local depth for the split_image_index;
    dir_page->IncrLocalDepth(split_image_index);
  }
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    KeyType new_key = hash_table_bucket_page->KeyAt(i);
    ValueType new_value = hash_table_bucket_page->ValueAt(i);
    hash_table_bucket_page->RemoveAt(i);
    page_id_t page_id = KeyToPageId(new_key, dir_page);
    if (page_id == bucket_page_id) {
      hash_table_bucket_page->Insert(new_key, new_value, comparator_);
    } else if (page_id == new_page_id) {
      new_hash_table_bucket_page->Insert(new_key, new_value, comparator_);
    }
  }
  // if is full?
  if (hash_table_bucket_page->IsFull()) {
    flag = SplitInsert(nullptr, key, value);
  } else {
    flag = hash_table_bucket_page->Insert(key, value, comparator_);
  }
  table_latch_.WUnlock();
  return flag;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  // first,find the directorypage
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  // second,find the page id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  // third,find the bucket_page
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);
  bool flag = hash_table_bucket_page->Remove(key, value, comparator_);
  table_latch_.RUnlock();
  if (hash_table_bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  return flag;
}

/*****************************************************************************
 * MERGE
 * Merge should be called after remove and the bucket is empty.
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  // first,find the directorypage
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  // second,find the page id
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  // third,find the bucket_page
  HASH_TABLE_BUCKET_TYPE *hash_table_bucket_page = FetchBucketPage(bucket_page_id);

  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  if (hash_table_bucket_page->IsEmpty() && dir_page->GetLocalDepth(bucket_index)) {
    // find the directory_index
    uint32_t split_image_index = dir_page->GetSplitImageIndex(bucket_index);
    if (dir_page->GetLocalDepth(split_image_index) == dir_page->GetLocalDepth(bucket_index)) {
      page_id_t page_id = dir_page->GetBucketPageId(split_image_index);
      // decresing local depth
      dir_page->DecrLocalDepth(bucket_index);
      dir_page->DecrLocalDepth(split_image_index);
      // set the bucket
      dir_page->SetBucketPageId(bucket_index, page_id);
      buffer_pool_manager_->DeletePage(bucket_page_id);
    }
  }
  table_latch_.WUnlock();
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
