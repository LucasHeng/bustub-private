//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  // ensure which element is occupied and readable
  // then make sure if it is matched
  bool flag = false;
  for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      if (cmp(key, array_[bucket_idx].first) == 0) {
        flag = true;
        result->emplace_back(array_[bucket_idx].second);
      }
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  bool flag = false;
  for (uint32_t bucket_idx = 0; bucket_idx != BUCKET_ARRAY_SIZE; bucket_idx++) {
    uint32_t idx = bucket_idx / 8;
    char pos = static_cast<char>(1 << (bucket_idx % 8));
    if (IsOccupied(bucket_idx)) {
      if (IsReadable(bucket_idx)) {
        auto &[key_, value_] = array_[bucket_idx];
        if (cmp(key_, key) == 0 && value_ == value) {
          return false;
        }
      } else {
        array_[bucket_idx] = std::make_pair(key, value);
        occupied_[idx] |= pos;
        readable_[idx] |= pos;
        flag = true;
        break;
      }
    } else {
      array_[bucket_idx] = std::make_pair(key, value);
      occupied_[idx] |= pos;
      readable_[idx] |= pos;
      flag = true;
      break;
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    auto &[key_, value_] = array_[bucket_idx];
    if (cmp(key, key_) == 0 && value == value_) {
      if (!IsReadable(bucket_idx)) {
        return false;
      }
      int idx = static_cast<int>(bucket_idx / 8);
      char pos = static_cast<char>(1 << (bucket_idx % 8));
      readable_[idx] &= (~(pos));
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  auto &key = array_[bucket_idx].first;
  return key;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  auto &value = array_[bucket_idx].second;
  return value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  int idx = static_cast<int>(bucket_idx / 8);
  char pos = static_cast<char>(1 << (bucket_idx % 8));
  readable_[idx] |= (~(pos));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  int idx = static_cast<int>(bucket_idx / 8);
  char pos = static_cast<char>(1 << (bucket_idx % 8));
  return (occupied_[idx] & pos) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  int idx = static_cast<int>(bucket_idx / 8);
  char pos = static_cast<char>(1 << (bucket_idx % 8));
  occupied_[idx] |= pos;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  int idx = static_cast<int>(bucket_idx / 8);
  char pos = static_cast<char>(1 << (bucket_idx % 8));
  return (readable_[idx] & pos) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  int idx = static_cast<int>(bucket_idx / 8);
  char pos = static_cast<char>(1 << (bucket_idx % 8));
  readable_[idx] |= pos;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  uint32_t size = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsReadable(bucket_idx)) {
      break;
    }
    size++;
  }
  return size == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t taken = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (IsReadable(bucket_idx)) {
      taken++;
    }
  }
  return taken;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (IsOccupied(bucket_idx)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
