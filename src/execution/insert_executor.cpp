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

#include <iostream>
#include <memory>

#include "catalog/catalog.h"
#include "common/logger.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (child_executor) {
    // 如果有child_executor
    child_executor_ = child_executor.release();
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
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool get_tuple_successed = false;
  bool insert_successed = false;
  if (has_child_executor_) {
    get_tuple_successed = child_executor_->Next(tuple, rid);
  } else {
    // 从rawalues_itrator中构造tuple,
    if (raw_values_iterator_ != plan_->RawValues().cend()) {
      *tuple = Tuple(*raw_values_iterator_, &table_info_->schema_);
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
    insert_successed = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    if (insert_successed) {
      // 插入成功则更新索引
      auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      // 更新所有的索引
      for (auto &index_info : indexs) {
        index_info->index_->InsertEntry(*tuple, *rid, exec_ctx_->GetTransaction());
      }
    }
  }

  return insert_successed;
}

}  // namespace bustub
