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
#include <utility>

#include "common/config.h"
#include "common/macros.h"
#include "storage/page/page.h"

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
  pages_ = new Page[pool_size_];  // 这里就是bufferpool中存放数据页的地方
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

//  FlushPgImp should flush a page regardless of its pin status.
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  bool is_flushed = false;
  // if (page_table_.count(page_id) != 0) {
  //   Page *page_to_flush = &pages_[page_table_[page_id]];
  //   disk_manager_->WritePage(page_id, page_to_flush->GetData());
  //   page_to_flush->is_dirty_ = false;
  //   is_flushed = true;
  // }
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    Page *page_to_flush = &pages_[iter->second];
    disk_manager_->WritePage(page_id, page_to_flush->GetData());
    page_to_flush->is_dirty_ = false;
    is_flushed = true;
  }
  return is_flushed;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);

  Page *page_to_flush;
  for (auto &element : page_table_) {
    page_to_flush = &pages_[element.second];
    disk_manager_->WritePage(element.first, page_to_flush->GetData());
    page_to_flush->is_dirty_ = false;  // 对比别人代码看出来的
  }
}

// Creates a new page in the buffer pool. ,注意这是创建一个新的页,得立即写回磁盘!
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  // latch_.lock();
  std::lock_guard<std::mutex> lock(latch_);
  // 1. 分配pageid
  // page_id_t page_id_just_allocated = AllocatePage(); //别在这里分配 ！！！ 否则在
  // ParallelBufferPoolManager的newpage测试中会有很大的pageid
  frame_id_t frame_id_to_place_new_page;
  Page *new_page = nullptr;
  // 2. 找一个空闲的frame
  if (FindFramePageId(&frame_id_to_place_new_page)) {
    // 3. 更新元数据并添加pagetable
    new_page = &pages_[frame_id_to_place_new_page];
    new_page->ResetMemory();
    page_id_t page_id_just_allocated = AllocatePage();
    new_page->page_id_ = page_id_just_allocated;
    new_page->is_dirty_ = false;  // 最后要写回磁盘，所以改成为false
    new_page->pin_count_++;
    // 在pagetable中添加frame_id和page_id的映射
    page_table_[page_id_just_allocated] = frame_id_to_place_new_page;
    // repalcer pin这个页面
    replacer_->Pin(frame_id_to_place_new_page);
    // 4.
    *page_id = page_id_just_allocated;
    // 5.将新页面写回磁盘
    disk_manager_->WritePage(page_id_just_allocated, new_page->GetData());
  }
  // latch_.unlock();
  return new_page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and
  // 4.     Insert P in pageTable
  // 5.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // latch_.lock();
  std::lock_guard<std::mutex> lock(latch_);
  Page *fetched_page = nullptr;
  frame_id_t frame_id_to_fetch;
  // 1.search in the  pagetable
  // if (page_table_.count(page_id) != 0) {
  //   // 1.1 在bufferpool中存在相应的page
  //   frame_id_to_fetch = page_table_[page_id];
  //   fetched_page = &pages_[frame_id_to_fetch];
  //   fetched_page->pin_count_++;         // pincount 加一
  //   replacer_->Pin(frame_id_to_fetch);  // 通知replaceer,不要将这个frame考虑在lru算法的范围内
  //   // latch_.unlock();
  //   return &pages_[frame_id_to_fetch];
  // }
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    // 1.1 在bufferpool中存在相应的page
    frame_id_to_fetch = iter->second;
    fetched_page = &pages_[frame_id_to_fetch];
    fetched_page->pin_count_++;         // pincount 加一
    replacer_->Pin(frame_id_to_fetch);  // 通知replaceer,不要将这个frame考虑在lru算法的范围内
    // latch_.unlock();
    return &pages_[frame_id_to_fetch];
  }
  // 1.2 bufferpool中不存在page,使用FindFramePageId辅助函数取得一个bufferpool中的空闲frameid
  // 2和3步骤 在 辅助函数中完成
  if (FindFramePageId(&frame_id_to_fetch)) {
    // 4. 在pagetable中添加映射
    page_table_[page_id] = frame_id_to_fetch;
    // 5.更新Page的元数据
    fetched_page = &pages_[frame_id_to_fetch];
    fetched_page->ResetMemory();
    fetched_page->page_id_ = page_id;
    fetched_page->is_dirty_ = false;
    fetched_page->pin_count_++;
    replacer_->Pin(frame_id_to_fetch);
    // 6. 将磁盘内容读到bufferpool中
    disk_manager_->ReadPage(page_id, fetched_page->GetData());
  }

  // 如果在bufferpool中的所有页都是被pin住了,表示失败,返回nullptr
  return fetched_page;
}

// 把bufferpool中的page移出
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  // if (page_table_.count(page_id) == 0) {
  //   // latch_.unlock();
  //   return true;
  // }
  // frame_id_t frame_id_to_delete = page_table_[page_id];
  // Page *page_to_delete = &pages_[frame_id_to_delete];
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id_to_delete = iter->second;
  Page *page_to_delete = &pages_[frame_id_to_delete];

  if (page_to_delete->pin_count_ != 0) {
    // some else is using the page
    return false;
  }

  if (page_to_delete->IsDirty()) {
    disk_manager_->WritePage(page_to_delete->page_id_, page_to_delete->data_);
  }

  page_table_.erase(page_id);
  replacer_->Pin(frame_id_to_delete);  // 让replacer不要再管理这个被释放的页了
  page_to_delete->ResetMemory();
  page_to_delete->page_id_ = INVALID_PAGE_ID;
  page_to_delete->is_dirty_ = false;
  page_to_delete->pin_count_ = 0;

  free_list_.push_back(frame_id_to_delete);

  return true;
}

// 这个函数的调用场景：如果本线程已经完成了对这个页的操作，unpin操作，isdirty表示，在本线程pin住这个页期间，有没有对它作写操作
// 这里强调本线程 ！！ 为什么？ 看看设置 is_dirty的逻辑。
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  // if (page_table_.count(page_id) == 0) {
  //   return false;
  // }
  // frame_id_t frame_id_unpin = page_table_[page_id];
  // Page *page_to_unpin = &pages_[frame_id_unpin];

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id_unpin = iter->second;
  Page *page_to_unpin = &pages_[frame_id_unpin];

  if (page_to_unpin->pin_count_ <= 0) {
    return false;
  }

  if (is_dirty) {
    //这里得判断
    // 如果本线程对这个页面做了写操作，那么里所应当得设置为脏
    // 如果本线程没有写这个页面什么都不要做！ 因为可能其他线程写了这个页！
    page_to_unpin->is_dirty_ = true;
  }

  page_to_unpin->pin_count_ -= 1;
  if (page_to_unpin->pin_count_ == 0) {
    // 如果pincount为0，那么通知replacer管理它
    replacer_->Unpin(frame_id_unpin);
  }
  // latch_.unlock();
  return true;
}

// 仅仅是返回pageid，如果有多个bufferpool实例，相当于用hash的方式将page分散在各个bufferpool中
page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

// 辅助函数,在freeList或replacer中返回一个空闲的frame_id
// 1. 在freelist中找空闲frame_id
// 2. 如果空闲链表为空，则使用 replacer 牺牲一个页
// 2.1 检查该页是否isDirty，如果是则调用disk_manager->WriteData()将数据写回，并重置pincount
// 3 在pagetable中删除对应的映射关系
bool BufferPoolManagerInstance::FindFramePageId(frame_id_t *frame_page_id) {
  // 这里不能再加锁了! 否则两个线程对一把锁加两次锁,容易死锁
  if (!free_list_.empty()) {
    // 空闲链表中有东西,直接返回true
    *frame_page_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  // 在lru中牺牲一个页
  if (!replacer_->Victim(frame_page_id)) {
    // 说明所有页都是被pin住的，不能移除
    return false;
  }
  // 要被牺牲的页面
  Page *page_to_victim = &pages_[*frame_page_id];
  // 或得牺牲页面的pageid
  page_id_t page_id_to_victim = page_to_victim->GetPageId();

  // 检查lru返回的那个页面的dirty标志,重置pincount
  // 好像没有必要reset pincount, 因为lru中维护的都是没有被pin住的frame_id
  if (page_to_victim->IsDirty()) {
    // 脏页,将它保存到磁盘上
    disk_manager_->WritePage(page_id_to_victim, page_to_victim->GetData());  // 写入到磁盘
    page_to_victim->pin_count_ = 0;
  }

  // 删除pagetable的映射
  page_table_.erase(page_id_to_victim);
  return true;
}

}  // namespace bustub
