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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <iterator>
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
  buffer_pool_manager_->NewPage(&bucket_page_id);

  new_hash_table_directory_page->SetBucketPageId(0, bucket_page_id);
  new_hash_table_directory_page->SetLocalDepth(0, 0);
  // unpin这两个页面
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
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
  auto bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  /* for debug*/
  // if (dir_page->GetGlobalDepth() == 1) {
  //   std::cout << "search key = " << key << std::endl;
  // }
  /* for debug*/

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool ret = bucket_page->GetValue(key, comparator_, result);  // 读取桶页内容前加页的读锁
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  /* for debug*/
  // if (!ret && dir_page->GetGlobalDepth() >= 1) {
  //   std::cout << "search failed!, search key = " << key << std::endl;
  // }
  /* for debug*/
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  // for debug
  // if (value == static_cast<ValueType>(252)) {
  //   std::cout << "key =  " << key << " value = " << value << std::endl;
  //   std::cout << "hash to page " << bucket_page_id << std::endl;
  //   // for debug
  // }
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool insert_successed = bucket_page->Insert(key, value, comparator_);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  // bucket 页面被修改了
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  // directory 页面没有被修改
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  // 一定要在 split insert之前释放 hash table的读锁，因为split insert 要写锁，不然就死锁了 ！
  if (!insert_successed && bucket_page->IsFull()) {
    // LOG_DEBUG("Split Inserting...");
    insert_successed = SplitInsert(transaction, key, value);
  }

  return insert_successed;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // LOG_DEBUG("!!!!!!!!Recursively CallSplit Insert()!!!!!!");
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  if (!bucket_page->IsFull()) {
    // 如果bucket_page没有满，则不用再分裂了， 相当于递归的中终止条件
    // LOG_DEBUG("NO Need to split");
    // std::cout << "last key : " << key << " hash to pageid = " << bucket_page_id << std::endl;
    bool ret = bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    // LOG_DEBUG("afer last key inserted, bucket condition : ");
    // bucket_page->PrintBucket();
    table_latch_.WUnlock();
    return ret;
  }

  // 根据该buckut页面的local_depth 是否等于 global_depth有两种做法
  //  如果local-depth == global_depth, 那么目录页面得增加一倍，然后分裂bucket，将原页面的键直对rehash
  //  如果local-depth < global_depth, 那么仅分裂bucket即可
  auto local_depth = dir_page->GetLocalDepth(bucket_idx);
  auto global_depth = dir_page->GetGlobalDepth();
  if (local_depth == global_depth) {
    // Directory Expansion
    // LOG_DEBUG("expension directory");
    ExpensionDirectory(dir_page);
    // LOG_DEBUG("after expension director, global depth = %d, diretory view:", dir_page->GetGlobalDepth());
    // if (dir_page->GetGlobalDepth() <= 5) {
    //   dir_page->PrintDirectory();
    // }
  }

  auto split_image_idx = dir_page->GetSplitImageIndex(bucket_idx);
  // LOG_DEBUG("bucket_idx = %d, image_idx = %d", bucket_idx, split_image_idx);
  page_id_t split_bucket_page_id;
  // 为分裂bucket创建新的页面
  HASH_TABLE_BUCKET_TYPE *image_bucket_page =
      reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
          buffer_pool_manager_->NewPage(&split_bucket_page_id)->GetData());
  // buffer_pool_manager_->NewPage(&split_bucket_page_id);
  // 在directory中设置bucket_image的 pageid
  // dir_page->SetBucketPageId(split_image_idx, split_bucket_page_id);
  auto local_mask_of_image = dir_page->GetLocalDepthMask(split_image_idx);
  auto local_mask_of_original = dir_page->GetLocalDepthMask(bucket_idx);
  // assert(local_mask_of_image == local_mask_of_original);

  // 将directory的下半部分对应的bucket的localdeoth加一，并且将所有的镜像页设置为对应的page_id
  for (uint32_t index = dir_page->Size() / 2; index < dir_page->Size(); index++) {
    if ((index & local_mask_of_image) == (split_image_idx & local_mask_of_image)) {
      dir_page->SetBucketPageId(index, split_bucket_page_id);
      dir_page->IncrLocalDepth(index);
    }
  }

  // 将direcotory的上半部分对应的bucket的localdepth加一
  for (uint32_t index = 0; index < dir_page->Size() / 2; index++) {
    if ((index & local_mask_of_original) == (bucket_idx & local_mask_of_original)) {
      dir_page->IncrLocalDepth(index);
    }
  }

  // LOG_DEBUG("IncrLocalDepth... and new bucket page, directory view : ");
  // dir_page->PrintDirectory();
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  reinterpret_cast<Page *>(image_bucket_page)->WLatch();

  // LOG_DEBUG("=========================Now Starting Rehashing !========================");
  // rehash所有原bucket中的元素 , 这个过程一定不会overflow
  for (uint32_t i = 0; i < static_cast<uint32_t>(BUCKET_ARRAY_SIZE); i++) {
    auto key_rehash = bucket_page->KeyAt(i);
    auto value_rehash = bucket_page->ValueAt(i);

    // for debug
    // auto rehash_bucket_idx = KeyToDirectoryIndex(key_rehash, dir_page);
    // if (rehash_bucket_idx < 0 || rehash_bucket_idx >= dir_page->Size()) {
    //   std::cout << "something wrong here, rehash_bucket_idx < 0  or rehash_bucket_idx >= direcory size\n";
    // }
    // for debug

    auto rehash_bucket_page_id = KeyToPageId(key_rehash, dir_page);

    if (rehash_bucket_page_id == bucket_page_id) {
      // std::cout << "key = " << key_rehash << " rehash to the original one" << std::endl;
      // 该元素被hash到原来的bucket中，就不用做什么了
      continue;
    }
    // 如果被hash到镜像bucket中，那么将它加入新的bucket，并且从老的bucket中删除
    // assert(rehash_bucket_page_id != split_bucket_page_id);  // 此时一定在镜像bucket中
    image_bucket_page->Insert(key_rehash, value_rehash, comparator_);
    // TODO(ljc): RemoveAt可能会更快
    bucket_page->Remove(key_rehash, value_rehash, comparator_);
  }
  // // LOG_DEBUG("===============Rehashing Complete!=================, direcotory page:");
  // if (dir_page->GetGlobalDepth() == 1) {
  //   dir_page->PrintDirectory();
  //   LOG_DEBUG("==========bucket page :  ");
  //   bucket_page->PrintBucket();
  //   LOG_DEBUG("==========image_bucket page :  ");
  //   image_bucket_page->PrintBucket();
  // }

  reinterpret_cast<Page *>(image_bucket_page)->WUnlatch();  // 注意加锁顺序
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(split_bucket_page_id, true);

  // 不要忘记unpin页面,这里有3个！
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  table_latch_.WUnlock();

  // 递归调用insert
  bool has_inserted = SplitInsert(transaction, key, value);
  return has_inserted;
}

// 自定义函数
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::ExpensionDirectory(HashTableDirectoryPage *dir_page) {
  auto directory_size_original = dir_page->Size();
  // globaldepth加一
  dir_page->IncrGlobalDepth();
  // auto global_depth_now = dir_page->GetGlobalDepth();
  auto directory_size_now = dir_page->Size();
  // 将原directory拷贝到现diretory的后半段中, 实际上拷贝的 bucket 的page_id，逻辑上是指针
  for (uint32_t i = 0, j = directory_size_original; i <= directory_size_original - 1 && j <= directory_size_now - 1;
       i++, j++) {
    dir_page->SetBucketPageId(j, dir_page->GetBucketPageId(i));
    dir_page->SetLocalDepth(j, dir_page->GetLocalDepth(i));
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucker_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucker_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  // LOG_DEBUG("remove hash to page_id = %d", bucker_page_id);
  bool has_deleted = bucket_page->Remove(key, value, comparator_);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  // 不要忘记unpin页面！！
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucker_page_id, true);
  table_latch_.RUnlock();
  // 在释放读锁后再调用merge，因为merge要获取写锁。 否则会引发死锁
  if (has_deleted && bucket_page->IsEmpty()) {
    // LOG_DEBUG("merge ! Before merge directory :");
    Merge(transaction, key, value);
    ExtraMerge(transaction, key, value);
  }
  return has_deleted;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_empty_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_empty_index = KeyToDirectoryIndex(key, dir_page);

  uint32_t bucket_image_index = dir_page->GetSplitImageIndex(bucket_empty_index);
  page_id_t bucket_image_page_id = dir_page->GetBucketPageId(bucket_image_index);

  auto ld_of_empty_bucket = dir_page->GetLocalDepth(bucket_empty_index);
  auto ld_of_image_bucket = dir_page->GetLocalDepth(bucket_image_index);

  // for debug
  LOG_DEBUG("Enter merge :");
  // for debug

  HASH_TABLE_BUCKET_TYPE *empty_bucket = FetchBucketPage(bucket_empty_page_id);
  // 判断是否能够合并
  if (ld_of_empty_bucket > 0 && ld_of_empty_bucket == ld_of_image_bucket &&
      bucket_empty_page_id != bucket_image_page_id &&
      empty_bucket
          ->IsEmpty()) {  // 再次判断bucket是否为空，是因为 remove 函数中是释放锁之后再 调用
                          // Merge的，可能这之间已经有其他线程插入了某个值。否则会出现某名奇妙的内存错误（本地测试）！
    // for debug
    LOG_DEBUG("Merge occur , before merge :");
    dir_page->PrintDirectory();
    uint32_t dir_size = dir_page->Size();
    for (uint32_t idx = 0; idx < dir_size; idx++) {
      auto bucket_page_id = dir_page->GetBucketPageId(idx);
      HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
      LOG_DEBUG("BucketIndex = %d" , idx);
      bucket_page->PrintBucket();
      buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
    }
    // for debug

    // 删除空余的bucket页
    reinterpret_cast<Page *>(empty_bucket)->WLatch();
    LOG_DEBUG("delete page_id %d", bucket_empty_page_id);
    buffer_pool_manager_->UnpinPage(bucket_empty_page_id, false);
    buffer_pool_manager_->DeletePage(bucket_empty_page_id);
    reinterpret_cast<Page *>(empty_bucket)->WUnlatch();

    auto local_mask = dir_page->GetLocalDepthMask(bucket_empty_index);
    // 设置对应bucket的pageid以及localdepth减1
    for (uint32_t index = 0; index < dir_page->Size(); index++) {
      if ((index & local_mask) == (bucket_empty_index & local_mask)) {
        dir_page->SetBucketPageId(index, bucket_image_page_id);
        dir_page->DecrLocalDepth(index);
      }
    }

    for (uint32_t index = 0; index < dir_page->Size(); index++) {
      if ((index & local_mask) == (bucket_image_index & local_mask)) {
        // dir_page->SetBucketPageId(index, bucket_image_page_id);
        dir_page->DecrLocalDepth(index);
      }
    }
    ShrinkDirectory(dir_page);
  } else {
    LOG_DEBUG("Didn't merge");
    LOG_DEBUG(
        "ld_of_empty_bucket = %d , ld_of_empty_bucket = %d : ld_of_image_bucket %d, bucket_empty_page_id = %d , "
        "bucket_image_page_id = %d",
        ld_of_empty_bucket, ld_of_empty_bucket, ld_of_image_bucket, bucket_empty_page_id, bucket_image_page_id);
    LOG_DEBUG("...");
  }
  // debug
  LOG_DEBUG("after merger directory");
  dir_page->PrintDirectory();
  uint32_t dir_size = dir_page->Size();
  for (uint32_t idx = 0; idx < dir_size; idx++) {
    auto bucket_page_id = dir_page->GetBucketPageId(idx);
    HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
    LOG_DEBUG("BucketIndex = %d" , idx);
    bucket_page->PrintBucket();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  }
  LOG_DEBUG("...");
  // debug
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();
}

// 自定义函数
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::ShrinkDirectory(HashTableDirectoryPage *dir_page) {
  // 如果局部深度都小于全局深度 ， 则全局深度减1
  auto global_depth = dir_page->GetGlobalDepth();
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetLocalDepth(i) == global_depth) {
      return;
    }
  }
  dir_page->DecrGlobalDepth();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::ExtraMerge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  // 扫描整个目录看看是否有空的bucket， 如果有则合并
  bool has_merged = false;      // 标记每次否合并操作
  bool has_merged_all = false;  // 标记整个循环是否有合并操作
  // extra merge 的 key 再次hash到的bucket一定不是空， 但是经过shrink后要检查这个桶的镜像桶是否为空，如果是则合并
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  for (uint32_t index = 0; index < dir_page->Size(); index++) {
    page_id_t bucket_to_be_detected_page_id = dir_page->GetBucketPageId(index);
    HASH_TABLE_BUCKET_TYPE *bucket_to_be_detected = FetchBucketPage(bucket_to_be_detected_page_id);
    if (bucket_to_be_detected->IsEmpty()) {
      uint32_t bucket_image_index = dir_page->GetSplitImageIndex(index);
      page_id_t bucket_image_page_id = dir_page->GetBucketPageId(bucket_image_index);

      auto ld_of_original_bucket = dir_page->GetLocalDepth(index);
      auto ld_of_image_bucket = dir_page->GetLocalDepth(bucket_image_index);
      // 判断是否能够合并
      if (ld_of_original_bucket > 0 && (ld_of_original_bucket == ld_of_image_bucket) &&
          (bucket_to_be_detected_page_id != bucket_image_page_id)) {
        has_merged = true;
        has_merged_all = true;
        // 删除空余的bucket页
        reinterpret_cast<Page *>(bucket_to_be_detected)->WLatch();
        LOG_DEBUG("delete pageid = %d", bucket_to_be_detected_page_id);
        buffer_pool_manager_->UnpinPage(bucket_to_be_detected_page_id, false);
        buffer_pool_manager_->DeletePage(bucket_to_be_detected_page_id);
        reinterpret_cast<Page *>(bucket_to_be_detected)->WUnlatch();

        for (uint32_t index = 0; index < dir_page->Size(); index++) {
          auto page_id_tetcting = dir_page->GetBucketPageId(index);
          if (page_id_tetcting == bucket_to_be_detected_page_id) {
            dir_page->SetBucketPageId(index, bucket_image_page_id);
            dir_page->DecrLocalDepth(index);
          } else if (page_id_tetcting == bucket_image_page_id) {
            dir_page->DecrLocalDepth(index);
          }
        }
        ShrinkDirectory(dir_page);
      }
    }
    if (has_merged) {
      has_merged = false;
      // 别忘了unpin， 这里卡了很久
      buffer_pool_manager_->UnpinPage(bucket_to_be_detected_page_id, true);
    } else {
      buffer_pool_manager_->UnpinPage(bucket_to_be_detected_page_id, false);
    }
  }
  if (has_merged_all) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  } else {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  }
  // buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();
  return has_merged;
}
// 用来debug
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintDir() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t dir_size = dir_page->Size();

  dir_page->PrintDirectory();
  // printf("dir size is: %d\n", dir_size);
  for (uint32_t idx = 0; idx < dir_size; idx++) {
    auto bucket_page_id = dir_page->GetBucketPageId(idx);
    HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
    bucket_page->PrintBucket();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  }

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}
/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
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
