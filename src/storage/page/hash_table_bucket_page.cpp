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
#include <cassert>
#include <iostream>
#include <utility>
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/hash_table_page_defs.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {
// 注意： 实现的是不重复key-value对的bucket页

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  return false;
}

// 注意 ： 如果桶满或者 
// 键重复都返回false ！ 在遍历空位时应该判断是否重复
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int readable_size = sizeof(readable_);
  assert(readable_size == (BUCKET_ARRAY_SIZE - 1) / 8 + 1);
  std::cout << "Inserting key = " << key << " : value = " << value << std::endl;
  for(int array_index = 0; array_index <= readable_size; array_index++) {
     char reading_bytes = readable_[array_index];
     for(int byte_idx = 0; byte_idx <= 7; byte_idx++) {
        if( static_cast<bool>(reading_bytes & (0B10000000 >> byte_idx))){
          //某一位已经被占用，判断是否重复,使用cmp比较
          if(cmp(array_[array_index * 8 + byte_idx].first, key) == 0) {
            return false;
          }
          //不重复则继续寻找下一个位置
          continue;
        }
        //找到空位，插入即可
        LOG_DEBUG("Found free space, array_index = %d, byte_index = %d",array_index, byte_idx);
        array_[array_index * 8 + byte_idx] = std::pair<KeyType, ValueType>(key,value);
        readable_[array_index] = readable_[array_index] | (0B10000000 >> byte_idx);
        //还要修改 occupied数组,将对应的位置1即可
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
  assert(readable_size == (BUCKET_ARRAY_SIZE - 1) / 8 + 1);
  
  for(int array_index = 0; array_index <= readable_size; array_index++) {
    char reading_bytes = readable_[array_index];
    char occupired_bytes = occupied_[array_index];
    for(int byte_idx = 0; byte_idx <= 7; byte_idx++) {
      if(! static_cast<bool>(occupired_bytes & (0B10000000 >> byte_idx))){
        //加速探测过程
        //occupied 的某个为为0 ， 表示这个为没有使用过，那么数组之后的元素也不可能使用过，直接返回false
        return false;
      }

      if(static_cast<bool>(reading_bytes & (0B10000000 >> byte_idx))){
        //这个位置还有元素，查看它的元素是否是要删除的元素
        if( cmp(array_[array_index * 8 + byte_idx].first , key)){
          //如果是则删除它，只需要将readable数组对应的位设置为0即可
          RemoveBit(&readable_[array_index],7 - byte_idx);
          assert((readable_[array_index] & (0B10000000 >> byte_idx)) == 0);
          return true;
        }
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
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  int byte_idx = static_cast<int>(bucket_idx / 8);
  int bit_idx =static_cast<int>(bucket_idx - byte_idx * 8);
  char byte = occupied_[byte_idx];
  return (byte & (0B10000000 >> bit_idx)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  int byte_idx = static_cast<int>(bucket_idx / 8);
  int bit_idx =static_cast<int>(bucket_idx - byte_idx * 8);
  char byte = readable_[byte_idx];
  return (byte & (0B10000000 >> bit_idx)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return false;
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
