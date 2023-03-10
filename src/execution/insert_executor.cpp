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
  if (child_executor_) {
    // 如果有child_executor，则表示上层调用者先用select从一张表中得到一些元组然后插入另一张表
    has_child_executor_ = true;
  } else {
    // 如果没有child_executor,则表示层层调用者自己输入插入的新元组
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
  bool get_tuple_successed = false; // 是否取得要插入的元组
  bool insert_successed = false;    // 插入是否成功

  if (has_child_executor_) {
    get_tuple_successed = child_executor_->Next(&tuple_to_be_inserted, &rid_to_be_inserted); // 从子执行器中取得tuple
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
        Tuple key_tuple = tuple_to_be_inserted.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                            index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key_tuple, rid_to_be_inserted, exec_ctx_->GetTransaction());
      }
    }
  }
  return insert_successed;
}

}  // namespace bustub
