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

#include <cassert>
#include <cstdint>
#include <iostream>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/page/hash_table_directory_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // 新建目录页 和 一个bucket页面，在目录页面的bucket_page_ids_数组中设置新的bucket页面的page_id
  auto new_hash_table_directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  // auto hash_table_bucket_page=
  // reinterpret_cast<HashTableBucketPage<KeyType,ValueType,KeyComparator>*>(buffer_pool_manager_->NewPage(&bucket_page_id)->GetData());
  buffer_pool_manager_->NewPage(&bucket_page_id);

  new_hash_table_directory_page->SetBucketPageId(0, bucket_page_id);
  new_hash_table_directory_page->SetLocalDepth(0, 0);
  // unpin这两个页面
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
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
  // 记住： bmp->fetch方法是会pin页面的，用完页面后即得unpin它，否则bufferpool空间容易满找不到空闲空间
  auto mask = dir_page->GetGlobalDepthMask();
  return Hash(key) & mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t directory_index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(directory_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  auto dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  // 待改进： 锁的粒度优化
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  bool got_value = bucket_page->GetValue(key, comparator_, result);
  // 一定要unpin ！！！
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.RUnlock();
  return got_value;
}

/*****************************************************************************
 * INSERTION
 * 先执行普通插入，如果没有成功则说明，bucket页面满了；那么再执行split_insert
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 待改进锁的粒度
  // table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  bool has_inserted = false;
  if (bucket_page->IsFull()) {
    has_inserted = SplitInsert(transaction, key, value);
  }
  has_inserted = bucket_page->Insert(key, value, comparator_);
  // 不要忘记unpin页面
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  // table_latch_.WUnlock();
  return has_inserted;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // TODO(ljc):  改进锁的粒度
  // table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  assert(bucket_page->IsFull());  // splitinsert 的时候这个页面一定是满的！

  // 根据该buckut页面的local_depth 是否等于 global_depth有两种做法
  //  如果local-depth == global_depth, 那么目录页面得增加一倍，然后分裂bucket，将原页面的键直对rehash
  //  如果local-depth < global_depth, 那么仅分裂bucket即可
  auto local_depth = dir_page->GetLocalDepth(bucket_idx);
  auto global_depth = dir_page->GetGlobalDepth();
  if (local_depth == global_depth) {
    // Directory Expansion
    ExpensionDirectory(dir_page);
  }
  auto split_image_idx = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t split_bucket_page_id;
  // 为分裂bucket创建新的页面
  HASH_TABLE_BUCKET_TYPE *image_bucket_page =
      reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
          buffer_pool_manager_->NewPage(&split_bucket_page_id)->GetData());
  // buffer_pool_manager_->NewPage(&split_bucket_page_id);
  // 在directory中设置bucket_image的 pageid
  dir_page->SetBucketPageId(split_image_idx, split_bucket_page_id);
  // 两个bucket的local_depth都加一
  dir_page->IncrLocalDepth(bucket_idx);
  dir_page->IncrLocalDepth(split_image_idx);

  // rehash所有原bucket中的元素 , 这个过程一定不会overflow
  for (uint32_t i = 0; i < static_cast<uint32_t>(BUCKET_ARRAY_SIZE); i++) {
    auto key_rehash = bucket_page->KeyAt(i);
    auto value_rehash = bucket_page->ValueAt(i);
    auto rehash_bucket_page_id = KeyToPageId(key_rehash, dir_page);

    if (rehash_bucket_page_id == bucket_page_id) {
      // 该元素被hash到原来的bucket中，就不用做什么了
      continue;
    }
    // 如果被hash到镜像bucket中，那么将它加入新的bucket，并且从老的bucket中删除
    assert(rehash_bucket_page_id == split_bucket_page_id);  // 此时一定在镜像bucket中
    image_bucket_page->Insert(key_rehash, value_rehash, comparator_);
    // TODO(ljc): RemoveAt可能会更快
    bucket_page->Remove(key_rehash, value_rehash, comparator_);
  }

  // 递归调用insert
  bool has_inserted = Insert(transaction, key, value);
  // 不要忘记unpin页面,这里有3个！
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(split_bucket_page_id, true);
  // table_latch_.WUnlock();
  return has_inserted;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::ExpensionDirectory(HashTableDirectoryPage *dir_page) {
  auto directory_size_original = dir_page->Size();
  // globaldepth加一
  dir_page->IncrGlobalDepth();
  // auto global_depth_now = dir_page->GetGlobalDepth();
  auto directory_size_now = dir_page->Size();
  // 将原directory拷贝到现diretory的后半端中, 实际上拷贝的 bucket 的page_id，逻辑上是指针
  for (uint32_t i = 0, j = directory_size_original; i <= directory_size_original - 1 && j <= directory_size_now - 1;
       i++, j++) {
    dir_page->SetBucketPageId(j, dir_page->GetBucketPageId(i));
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucker_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucker_page_id);
  bool has_deleted = bucket_page->Remove(key, value, comparator_);
  // if(bucket_page->IsEmpty()) {}
  //  不要忘记unpin页面！！

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucker_page_id, true);
  // table_latch_.WUnlock();
  return has_deleted;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

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
