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
  int index = -1;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (IsReadable(bucket_idx)) {
      auto &[key_, value_] = array_[bucket_idx];
      if (cmp(key_, key) == 0 && value_ == value) {
        return false;
      }
    } else if (index == -1) {
      index = bucket_idx;
    }
  }
  if (index != -1) {
    array_[index] = static_cast<MappingType>(std::make_pair(key, value));
    SetOccupied(index);
    SetReadable(index);
    flag = true;
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    auto &[key_, value_] = array_[bucket_idx];
    if (IsReadable(bucket_idx)) {
      if (cmp(key, key_) == 0 && value == value_) {
        RemoveAt(bucket_idx);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  int idx = static_cast<int>(bucket_idx / 8);
  char pos = static_cast<char>(0x1 << (bucket_idx & 0x7));
  readable_[idx] &= (~(pos));
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
  uint8_t mask = static_cast<uint8_t>((1 << 8) - 1);
  size_t iter_nums = BUCKET_ARRAY_SIZE / 8;
  for (size_t num = 0; num < iter_nums; ++num) {
    // if (!IsReadable(bucket_idx)) {
    //   return false;
    // }
    // 提高效率，一次多一点，而不是一位一位运算
    uint8_t state = static_cast<uint8_t>(readable_[num]);
    if (state != mask) {
      return false;
    }
  }

  size_t last_remain = BUCKET_ARRAY_SIZE % 8;
  if (last_remain > 0) {
    uint8_t state = static_cast<uint8_t>(readable_[iter_nums]);
    for (size_t i = 0; i < last_remain; i++) {
      if ((state & 1) != 1) {
        return false;
      }
      state >>= 1;
    }
  }
  return true;
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
  uint8_t mask = 255;
  size_t iter_nums = (BUCKET_ARRAY_SIZE - 1) / 8;
  for (size_t i = 0; i <= iter_nums; i++) {
    if ((readable_[i] & mask) > 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::vector<MappingType> HASH_TABLE_BUCKET_TYPE::GetAllItem() {
  uint32_t bucket_size = BUCKET_ARRAY_SIZE;
  std::vector<MappingType> items;
  items.reserve(bucket_size);
  for (uint32_t i = 0; i < bucket_size; i++) {
    if (IsReadable(i)) {
      items.emplace_back(array_[i]);
    }
  }
  return items;
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
