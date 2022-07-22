![img](./project1_submmit1.png)


![img](./project1_submmit1_info1.png)

好像看不出啥
本地测试看出来了，就是因为在 newpgimg时，一开始就调用了AllocatePage(),但是失败了却没有回滚，正确做法是在分配成功后再调用AllocatePage()

~~~cpp
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  // 1. 分配pageid
  // page_id_t page_id_just_allocated = AllocatePage(); //别在这里分配 ！！！ 否则在 ParallelBufferPoolManager的newpage测试中会有很大的pageid
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

  latch_.unlock();
  return new_page;
}
~~~cpp

