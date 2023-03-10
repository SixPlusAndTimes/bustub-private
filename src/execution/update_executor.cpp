//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>

#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "common/logger.h"
namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool get_tuple_succeed = false;
  bool update_succeed = false;
  Tuple tuple_to_be_updated;
  RID rid_to_be_updated;
  get_tuple_succeed = child_executor_->Next(&tuple_to_be_updated, &rid_to_be_updated);

  if (get_tuple_succeed) {
    // 只有REPEATABLE_READ 隔离级别下才会锁升级，因为只有在这个级别下，读锁不会释放直到事务commit
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), rid_to_be_updated);
    } else {
      // 其他隔离级别下直接加写锁
      exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), rid_to_be_updated);
    }
    Tuple updated_tuple = GenerateUpdatedTuple(tuple_to_be_updated);  // 生成新的tuple
    bool update_tuple_in_heap_succeed =
        table_info_->table_->UpdateTuple(updated_tuple, rid_to_be_updated, exec_ctx_->GetTransaction());
    if (update_tuple_in_heap_succeed) {
      // 根新索引
      for (auto &index_info : indexs_info_) {
        Tuple old_key_tuple = tuple_to_be_updated.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                               index_info->index_->GetKeyAttrs());
        Tuple new_key_tuple = updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                         index_info->index_->GetKeyAttrs());
        // 先删除然后再插入达到更新的目的
        index_info->index_->DeleteEntry(old_key_tuple, rid_to_be_updated, exec_ctx_->GetTransaction());
        index_info->index_->InsertEntry(new_key_tuple, rid_to_be_updated, exec_ctx_->GetTransaction());
      }
      update_succeed = true;
    }
  }
  return update_succeed;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
