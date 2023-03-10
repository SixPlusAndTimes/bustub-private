//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"

namespace bustub {
// 要删除的tuple总是来自于seqscan子执行器，将对应的rid的tuple， `Mark delete `就行了
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool delete_successed = false;
  Tuple tuple_to_be_delete;
  RID rid_to_be_delete;
  bool get_rid_successed = child_executor_->Next(&tuple_to_be_delete, &rid_to_be_delete);
  // 只有REPEATABLE_READ 隔离级别下才会锁升级
  if (get_rid_successed && exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), rid_to_be_delete);
  } else {
    exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), rid_to_be_delete);
  }
  
  if (get_rid_successed) {
    bool tuple_deleted = table_info_->table_->MarkDelete(rid_to_be_delete, exec_ctx_->GetTransaction());
    if (tuple_deleted) {
      for (auto &index : indexs_info_) {  // 更新索引
        Tuple key_tuple =
            tuple_to_be_delete.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key_tuple, rid_to_be_delete, exec_ctx_->GetTransaction());
        // 添加索引修改记录
        exec_ctx_->GetTransaction()->AppendTableWriteRecord(IndexWriteRecord{
            rid_to_be_delete, plan_->TableOid(), WType::DELETE, key_tuple, index->index_oid_, exec_ctx_->GetCatalog()});
      }
      delete_successed = true;
    } else {
      throw Exception("delte fail");
    }
  }
  return delete_successed;
}

}  // namespace bustub
