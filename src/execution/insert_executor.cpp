//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <utility>

#include "catalog/catalog.h"
#include "common/logger.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // 注意： 这里可能发生内存泄漏， 要在.h 中声明智能指针来管理child_executor
  if (child_executor_) {
    // 如果有child_executor
    has_child_executor_ = true;
  } else {
    // rawinsert 就初始使化raw_values_iterator
    // 注意C++ 语法 cbegin 与 const iterator
    // https://blog.csdn.net/caroline_wendy/article/details/16030561
    raw_values_iterator_ = plan_->RawValues().cbegin();
  }
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());  // 要插入的表的信息
}

void InsertExecutor::Init() {
  if (has_child_executor_) {  // 非rawinsert ， 初始化子节点
    child_executor_->Init();
  }
}

// rid 在真正执行插入表时才会被赋值
// "InsertExecutor should not modify the result set", 所以Next的两个参数是没有用的，不要将它们赋值
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tuple_to_be_inserted;
  RID rid_to_be_inserted;
  bool get_tuple_successed = false;
  bool insert_successed = false;
  if (has_child_executor_) {
    get_tuple_successed = child_executor_->Next(&tuple_to_be_inserted, &rid_to_be_inserted);
  } else {
    // 从rawalues_itrator中构造tuple,
    if (raw_values_iterator_ != plan_->RawValues().cend()) {
      tuple_to_be_inserted = Tuple(*raw_values_iterator_, &table_info_->schema_);
      raw_values_iterator_++;
      get_tuple_successed = true;
    } else {
      // 没有元素可以迭代了
      get_tuple_successed = false;
    }
  }

  if (get_tuple_successed) {
    // 在tableheap中执行真正的插入
    // LOG_DEBUG("Insert in Table heap");
    insert_successed =
        table_info_->table_->InsertTuple(tuple_to_be_inserted, &rid_to_be_inserted, exec_ctx_->GetTransaction());
    if (insert_successed) {
      // 插入成功则更新索引
      auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      // 更新所有的索引
      for (auto &index_info : indexs) {
        // auto index = index_info->index_;
        // auto index_schema = index_info->key_schema_;
        Tuple key_tuple = tuple_to_be_inserted.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                            index_info->index_->GetKeyAttrs());
        // 见可扩展hash索引的源文件， key就是 key_tuple, value 就是 rid_to_be_inserted
        // 而 rid 就是原表元组的物理位置， 符合预期，因为给出一个索引的key就会得到原表元组的位置信息。
        // Rid 又包括 page_id 和 slotnum， 我们可以很方便地找到这个元组，而不用顺序扫描所有记录
        index_info->index_->InsertEntry(key_tuple, rid_to_be_inserted, exec_ctx_->GetTransaction());
      }
    }
  }
  return insert_successed;
}

}  // namespace bustub
