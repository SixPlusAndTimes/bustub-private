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

#include <sys/types.h>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <ostream>
#include <utility>
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/page/hash_table_page_defs.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {
// 注意： 实现的是可重复的key，但是不能有重复的key-value对

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  // 找到在bucket中所有key符合条件的value 并存储在 result中
  for (uint32_t index = 0; index < static_cast<uint32_t>(BUCKET_ARRAY_SIZE); index++) {
    if (IsReadable(index)) {
      if (cmp(key, KeyAt(index)) == 0) {
        result->push_back(array_[index].second);
      }
    } else if (!IsOccupied(index)) {  // occupied数组加速查找
      break;
    }
  }
  return !static_cast<bool>(result->empty());
}

// 注意 ： 如果桶满或者
// 键重复都返回false ！ 在遍历空位时应该判断是否重复
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int readable_size = sizeof(readable_);
  // assert(readable_size == (BUCKET_ARRAY_SIZE - 1) / 8 + 1);
  for (int array_index = 0; array_index < readable_size; array_index++) {
    char reading_bytes = readable_[array_index];
    for (int byte_idx = 0; byte_idx <= 7; byte_idx++) {
      if (static_cast<bool>(reading_bytes & (0B10000000 >> byte_idx))) {
        // 某一位已经被占用，判断是否重复,使用cmp比较key，使用 == 号比较value
        if ((cmp(array_[array_index * 8 + byte_idx].first, key) == 0) &&
            array_[array_index * 8 + byte_idx].second == value) {
          return false;
        }
        // 不重复则继续寻找下一个空位置
        continue;
      }
      // 找到空位，插入即可
      // LOG_DEBUG("Found free space, array_index = %d, byte_index = %d",array_index, byte_idx);
      array_[array_index * 8 + byte_idx] = std::pair<KeyType, ValueType>(key, value);
      readable_[array_index] = readable_[array_index] | (0B10000000 >> byte_idx);
      // 还要修改 occupied数组,将对应的位置1即可
      occupied_[array_index] = occupied_[array_index] | (0B10000000 >> byte_idx);
      return true;
    }
  }
  return false;
}

/**
 * Removes a key and value.
 *
 * @return true if removed, false if not found
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  int readable_size = sizeof(readable_);
  // assert(readable_size == (BUCKET_ARRAY_SIZE - 1) / 8 + 1);

  for (int array_index = 0; array_index <= readable_size; array_index++) {
    char reading_bytes = readable_[array_index];
    char occupired_bytes = occupied_[array_index];
    for (int byte_idx = 0; byte_idx <= 7; byte_idx++) {
      /*  */
      if (!static_cast<bool>(occupired_bytes & (0B10000000 >> byte_idx))) {
        // 加速探测过程
        // occupied 的某个为为0 ， 表示这个为没有使用过，那么数组之后的元素也不可能使用过，直接返回false
        return false;
      }

      if (static_cast<bool>(reading_bytes & (0B10000000 >> byte_idx))) {
        // 这个位置还有元素，查看它的元素是否是要删除的元素
        if ((cmp(array_[array_index * 8 + byte_idx].first, key) == 0) &&
            array_[array_index * 8 + byte_idx].second == value) {
          // std::cout << "remove key = " << key << " remove value = " << value <<std::endl;
          // LOG_DEBUG("Rmove Element , array index = %d, bit index = %d", array_index, byte_idx);
          // std::cout << "actual remove key = " << array_[array_index * 8 + byte_idx].first <<std::endl;
          // 如果是则删除它，只需要将readable数组对应的位设置为0即可
          RemoveBit(&readable_[array_index], 7 - byte_idx);
          // std::cout << "readable_[array_index] & (0B10000000 >> byte_idx) = " << (readable_[array_index] &
          // (0B10000000 >> byte_idx)) <<std::endl; assert((readable_[array_index] & (0B10000000 >> byte_idx)) == 0);
          return true;
        }
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].first;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].second;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetUnreadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t byte_idx = bucket_idx / 8;
  uint32_t bit_idx = bucket_idx - byte_idx * 8;
  char byte = occupied_[byte_idx];
  return (byte & (0B10000000 >> bit_idx)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t byte_idx = static_cast<int>(bucket_idx / 8);
  uint32_t bit_idx = static_cast<int>(bucket_idx % 8);
  occupied_[byte_idx] |= (0B10000000 >> bit_idx);
  assert(IsOccupied(bucket_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t byte_idx = bucket_idx / 8;
  uint32_t bit_idx = bucket_idx % 8;
  char byte = readable_[byte_idx];
  // LOG_DEBUG("IsReadable(): byteindex = %d, bitindex = %d", byte_idx, bit_idx);
  // std::cout << "byte & (0B10000000 >> bit_idx) = " << (byte & (0B10000000 >> bit_idx)) << std::endl;
  return (byte & (0B10000000 >> bit_idx)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t byte_idx = static_cast<int>(bucket_idx / 8);
  uint32_t bit_idx = static_cast<int>(bucket_idx % 8);
  readable_[byte_idx] |= (0B10000000 >> bit_idx);
  assert(IsReadable(bucket_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  for (uint32_t index = 0; index < static_cast<uint32_t>(BUCKET_ARRAY_SIZE); index++) {
    // TODO(ljc): 修改成操作字节可能更快？
    if (IsReadable(index)) {
      // 这一位可读，就是说这一位有数据存储
      continue;
    }
    return false;
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t num_readable = 0;
  for (uint32_t index = 0; index < static_cast<uint32_t>(BUCKET_ARRAY_SIZE); index++) {
    if (IsReadable(index)) {
      ++num_readable;
    }
  }
  return num_readable;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (uint32_t index = 0; index < static_cast<uint32_t>(BUCKET_ARRAY_SIZE); index++) {
    if (IsReadable(index)) {
      return false;
    }
  }
  return true;
}

/** 自定义函数 **/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::Size() {  // 返回桶的大小
  return BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnreadable(uint32_t bucket_idx) {
  // 比如 bucket_idx =  23 => byte_idx = 2 , bit_idx = 7
  uint32_t byte_index = bucket_idx / 8;
  uint32_t bit_index = bucket_idx % 8;
  readable_[byte_index] &= ~(0B10000000 >> bit_index);
  assert(!IsReadable(bucket_idx));
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
void HASH_TABLE_BUCKET_TYPE::RemoveBit(char *value, int index) {
  char bit = static_cast<char>(1 << index);
  char mask = 0;
  // ^ 异或操作
  // ～ 桉位取反
  mask = static_cast<char>((~mask) ^ bit);
  *value = static_cast<char>(*value & mask);
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
