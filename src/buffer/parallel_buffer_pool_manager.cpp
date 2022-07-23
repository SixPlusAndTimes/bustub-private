//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include <sys/types.h>
#include <cstddef>
#include <new>
#include <vector>
#include "buffer/buffer_pool_manager_instance.h"
// #include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances), pool_size_(pool_size), start_index_(0) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances; i++) {
    // BufferPoolManagerInstance buffer_pool(pool_size,num_instances,i, disk_manager,log_manager);
    BufferPoolManagerInstance *bmp =
        new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    buffer_pool_managers_.push_back(bmp);
  }
  // LOG_DEBUG("num_instandes = %zu, pool_size = %zu \n",num_instances, pool_size);
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete buffer_pool_managers_[i];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  int index_of_instance = static_cast<int>(page_id % num_instances_);
  return buffer_pool_managers_[index_of_instance];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  Page *new_page = nullptr;
  size_t index = start_index_;
  do {
    new_page = buffer_pool_managers_[index]->NewPage(page_id);
    if (new_page != nullptr) {
      break;
    }
    index = (index + 1) % num_instances_;
  } while (index != start_index_);
  start_index_ = (start_index_ + 1) % num_instances_;
  // LOG_DEBUG("ParallelBufferPoolManager::NewPgImp  getPage? : %d (1 : get, 0 : not get) ",new_page != nullptr);
  // LOG_DEBUG("ParallelBufferPoolManager::NewPgImp, get pageId = %d", *page_id);

  return new_page;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (BufferPoolManager *bmp : buffer_pool_managers_) {
    bmp->FlushAllPages();
  }
}

}  // namespace bustub
